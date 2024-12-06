# This file is part of prompt_processing.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = ["next_visit_handler"]

import collections.abc
import contextlib
import functools
import json
import logging
import os
import signal
import socket
import sys
import time
import uuid
import yaml

import boto3
from botocore.handlers import validate_bucket_name
import cloudevents.http
import confluent_kafka as kafka
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
import flask

from .config import PipelinesConfig
from .exception import GracefulShutdownInterrupt, IgnorableVisit, InvalidVisitError, \
    NonRetriableError, RetriableError
from .logger import setup_usdf_logger
from .middleware_interface import get_central_butler, \
    make_local_repo, make_local_cache, MiddlewareInterface
from .raw import (
    check_for_snap,
    is_path_consistent,
    get_group_id_from_oid,
)
from .repo_tracker import LocalRepoTracker
from .visit import FannedOutVisit

# Platform that prompt processing will run on
platform = os.environ["PLATFORM"].lower()
# The short name for the instrument.
instrument_name = os.environ["RUBIN_INSTRUMENT"]
# The skymap to use in the central repo
skymap = os.environ["SKYMAP"]
# URI to the main repository containing calibs and templates
calib_repo = os.environ["CALIB_REPO"]
# S3 Endpoint for Buckets; needed for direct Boto access but not Butler
s3_endpoint = os.environ["S3_ENDPOINT_URL"]
# URI of raw image microservice
raw_microservice = os.environ.get("RAW_MICROSERVICE", "")
# Bucket name (not URI) containing raw images
image_bucket = os.environ["IMAGE_BUCKET"]
# Time to wait after expected script completion for image arrival, in seconds
image_timeout = int(os.environ.get("IMAGE_TIMEOUT", 20))
# Absolute path on this worker's system where local repos may be created
local_repos = os.environ.get("LOCAL_REPOS", "/tmp")
# Kafka server
kafka_cluster = os.environ["KAFKA_CLUSTER"]
# Kafka group; must be worker-unique to keep workers from "stealing" messages for others.
kafka_group_id = str(uuid.uuid4())
# The topic on which to listen to updates to image_bucket
bucket_topic = os.environ.get("BUCKET_TOPIC", "rubin-prompt-processing")
# Offset for Kafka bucket notification.
bucket_notification_kafka_offset_reset = os.environ.get("BUCKET_NOTIFICATION_KAFKA_OFFSET_RESET", "latest")

# Conditionally load keda environment variables
if platform == "keda":
    # Kafka Schema Registry URL for next visit fan out messages
    fan_out_schema_registry_url = os.environ["FAN_OUT_SCHEMA_REGISTRY_URL"]
    # Kafka cluster with next visit fanned out messages.
    fan_out_kafka_cluster = os.environ["FAN_OUT_KAFKA_CLUSTER"]
    # Kafka group for next visit fan out messages.
    fan_out_kafka_group_id = os.environ["FAN_OUT_KAFKA_GROUP_ID"]
    # Kafka topic for next visit fan out messages.
    fan_out_kafka_topic = os.environ["FAN_OUT_KAFKA_TOPIC"]
    # Kafka topic offset for next visit fan out messages.
    fan_out_kafka_topic_offset = os.environ["FAN_OUT_KAFKA_TOPIC_OFFSET"]
    # Kafka Fan Out SASL Mechansim.
    fan_out_kafka_sasl_mechanism = os.environ["FAN_OUT_KAFKA_SASL_MECHANISM"]
    # Kafka Fan Out Security Protocol.
    fan_out_kafka_security_protocol = os.environ["FAN_OUT_KAFKA_SECURITY_PROTOCOL"]
    # Kafka Fan Out Consumer Username.
    fan_out_kafka_sasl_username = os.environ["FAN_OUT_KAFKA_SASL_USERNAME"]
    # Kafka Fan Out Consumer Password.
    fan_out_kafka_sasl_password = os.environ["FAN_OUT_KAFKA_SASL_PASSWORD"]
    # Time to wait for fanned out messages before spawning new pod.
    fanned_out_msg_listen_timeout = int(os.environ.get("FANNED_OUT_MSG_LISTEN_TIMEOUT", 300))

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def _config_from_yaml(yaml_string):
    """Initialize a PipelinesConfig from a YAML-formatted string.

    Parameters
    ----------
    yaml_string : `str`
        A YAML representation of the structured config. See
        `~activator.config.PipelineConfig` for details.

    Returns
    -------
    config : `activator.config.PipelineConfig`
        The corresponding config object.
    """
    return PipelinesConfig(yaml.safe_load(yaml_string))


# The preprocessing pipelines to execute and the conditions in which to choose them.
pre_pipelines = _config_from_yaml(os.environ["PREPROCESSING_PIPELINES_CONFIG"])
# The main pipelines to execute and the conditions in which to choose them.
main_pipelines = _config_from_yaml(os.environ["MAIN_PIPELINES_CONFIG"])


def find_local_repos(base_path):
    """Search for existing local repos.

    Parameters
    ----------
    base_path : `str`
        The directory in which to search for repos.

    Returns
    -------
    repos : collection [`str`]
        The root directories of any local repos found.
    """
    subdirs = {entry.path for entry in os.scandir(base_path) if entry.is_dir()}
    return {d for d in subdirs if os.path.exists(os.path.join(d, "butler.yaml"))}


@functools.cache
def _get_consumer():
    """Lazy initialization of shared Kafka Consumer."""
    return kafka.Consumer({
        "bootstrap.servers": kafka_cluster,
        "group.id": kafka_group_id,
        "auto.offset.reset": bucket_notification_kafka_offset_reset,
    })


@functools.cache
def _get_storage_client():
    """Lazy initialization of cloud storage reader."""
    storage_client = boto3.client('s3', endpoint_url=s3_endpoint)
    storage_client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)
    return storage_client


@functools.cache
def _get_central_butler():
    """Lazy initialization of central Butler."""
    return get_central_butler(calib_repo, instrument_name)


@functools.cache
def _get_local_repo():
    """Lazy initialization of local repo.

    Returns
    -------
    repo : `tempfile.TemporaryDirectory`
        The directory containing the repo, to be removed when the
        process exits.
    """
    repo = make_local_repo(local_repos, _get_central_butler(), instrument_name)
    tracker = LocalRepoTracker.get()
    tracker.register(os.getpid(), repo.name)
    return repo


@functools.cache
def _get_local_cache():
    """Lazy initialization of local repo dataset cache."""
    return make_local_cache()


def create_app():
    try:
        setup_usdf_logger(
            labels={"instrument": instrument_name},
        )

        # Check initialization and abort early
        _get_consumer()
        _get_storage_client()
        _get_central_butler()
        _get_local_repo()

        app = flask.Flask(__name__)
        app.add_url_rule("/next-visit", view_func=next_visit_handler, methods=["POST"])
        app.register_error_handler(IgnorableVisit, skip_visit)
        app.register_error_handler(InvalidVisitError, invalid_visit)
        app.register_error_handler(RetriableError, request_retry)
        app.register_error_handler(NonRetriableError, forbid_retry)
        app.register_error_handler(500, server_error)
        _log.info("Worker ready to handle requests.")
        return app
    except Exception as e:
        _log.critical("Failed to start worker; aborting.")
        _log.exception(e)
        # gunicorn assumes exit code 3 means "Worker failed to boot", though this is not documented
        sys.exit(3)


def dict_to_fanned_out_visit(obj, ctx):
    """
    Converts object literal(dict) to a Fanned Out instance.
    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        obj (dict): Object literal(dict)
    """

    if obj is None:
        return None

    return FannedOutVisit(**obj)


def keda_start():

    try:
        setup_usdf_logger(
            labels={"instrument": instrument_name},
        )

        # Initialize local registry
        registry = LocalRepoTracker.get()
        registry.init_tracker()

        # Check initialization and abort early
        _get_consumer()
        _get_storage_client()
        _get_central_butler()
        _get_local_repo()

        _log.info("Worker ready to handle requests.")

    except Exception as e:
        _log.critical("Failed to start worker; aborting.")
        _log.exception(e)
        sys.exit(1)

    # Initialize schema registry for fan out
    fan_out_schema_registry_conf = {'url': fan_out_schema_registry_url}
    fan_out_schema_registry_client = SchemaRegistryClient(fan_out_schema_registry_conf)

    fan_out_avro_deserializer = AvroDeserializer(schema_registry_client=fan_out_schema_registry_client,
                                                 from_dict=dict_to_fanned_out_visit)
    fan_out_consumer_conf = {
        "bootstrap.servers": fan_out_kafka_cluster,
        "group.id": fan_out_kafka_group_id,
        "auto.offset.reset": fan_out_kafka_topic_offset,
        "sasl.mechanism": fan_out_kafka_sasl_mechanism,
        "security.protocol": fan_out_kafka_security_protocol,
        "sasl.username": fan_out_kafka_sasl_username,
        "sasl.password": fan_out_kafka_sasl_password,
        'enable.auto.commit': False
    }

    _log.info("starting fan out consumer")
    fan_out_consumer = kafka.Consumer(fan_out_consumer_conf, logger=_log)
    fan_out_consumer.subscribe([fan_out_kafka_topic])
    fan_out_listen_start_time = time.time()

    try:
        while time.time() - fan_out_listen_start_time < fanned_out_msg_listen_timeout:

            fan_out_message = fan_out_consumer.poll(timeout=5)
            if fan_out_message is None:
                continue
            if fan_out_message.error():
                _log.warning("Fanned out consumer error: %s", fan_out_message.error())
            else:
                deserialized_fan_out_visit = fan_out_avro_deserializer(fan_out_message.value(),
                                                                       SerializationContext(
                                                                       fan_out_message.topic(),
                                                                       MessageField.VALUE))
                _log.info("Unpacked message as %r.", deserialized_fan_out_visit)

                # Calculate time to load knative and receive message based on timestamp in Kafka message
                _log.debug("Message timestamp %r", fan_out_message.timestamp())
                fan_out_kafka_msg_timestamp = fan_out_message.timestamp()
                fan_out_to_prompt_time = int(time.time() * 1000) - fan_out_kafka_msg_timestamp[1]
                _log.debug("Seconds since fan out message delivered %r", fan_out_to_prompt_time/1000)

                # Commit message and close client
                fan_out_consumer.commit(message=fan_out_message, asynchronous=False)
                fan_out_consumer.close()

                try:
                    # Process fan out visit
                    process_visit(deserialized_fan_out_visit)
                except Exception as e:
                    _log.critical("Process visit failed; aborting.")
                    _log.exception(e)
                finally:
                    _log.info("Processing completed for %s", socket.gethostname())
                    break

    finally:
        # TODO Handle local registry unregistration on DM-47975
        _log.info("Finished listening for fanned out messages")


def _graceful_shutdown(signum: int, stack_frame):
    """Signal handler for cases where the service should gracefully shut down.

    Parameters
    ----------
    signum : `int`
        The signal received.
    stack_frame : `frame` or `None`
        The "current" stack frame.

    Raises
    ------
    activator.exception.GracefulShutdownInterrupt
        Raised unconditionally.
    """
    signame = signal.Signals(signum).name
    _log.info("Signal %s detected, cleaning up and shutting down.", signame)
    # TODO DM-45339: raising in signal handlers is dangerous; can we get a way
    # for pipeline processing to check for interrupts?
    raise GracefulShutdownInterrupt(f"Received signal {signame}.")


def with_signal(signum: int,
                handler: collections.abc.Callable | signal.Handlers,
                ) -> collections.abc.Callable:
    """A decorator that registers a signal handler for the duration of a
    function call.

    Parameters
    ----------
    signum : `int`
        The signal for which to register a handler; see `signal.signal`.
    handler : callable or `signal.Handlers`
        The handler to register.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            old_handler = signal.signal(signum, handler)
            try:
                return func(*args, **kwargs)
            finally:
                if old_handler is not None:
                    signal.signal(signum, old_handler)
                else:
                    signal.signal(signum, signal.SIG_DFL)
        return wrapper
    return decorator


def parse_next_visit(http_request):
    """Parse a next_visit event and extract its data.

    Parameters
    ----------
    http_request : `flask.Request`
        The request to be parsed.

    Returns
    -------
    next_visit : `activator.visit.FannedOutVisit`
        The next_visit message contained in the request.

    Raises
    ------
    ValueError
        Raised if ``http_request`` is not a valid message.
    """
    event = cloudevents.http.from_http(http_request.headers, http_request.get_data())
    if not event:
        raise ValueError("no CloudEvent received")
    if not event.data:
        raise ValueError("empty CloudEvent received")

    # Message format is determined by the nextvisit-start deployment.
    data = json.loads(event.data)
    visit = FannedOutVisit(**data)
    _log.debug("Unpacked message as %r.", visit)
    return visit


def _filter_messages(messages):
    """Split Kafka output into proper messages and errors.

    This function returns the proper messages, and logs the errors.

    Parameters
    ----------
    messages : `list` [`confluent_kafka.Message`]
        The messages to filter.

    Returns
    -------
    cleaned_messages : `list` [`confluent_kafka.Message`]
        The received messages from within ``messages``.
    """
    cleaned = []
    for msg in messages:
        if msg.error():
            # TODO: not all error() are actually *errors*
            _log.warning("Consumer event: %s", msg.error())
        else:
            cleaned.append(msg)
    return cleaned


def _parse_bucket_notifications(payload):
    """Extract object IDs from an S3 notification.

    If one record is invalid, an error is logged but the function tries to
    process the remaining records.

    Do not return notifications from "sidecar" JSON files.

    Parameters
    ----------
    payload : `str` or `bytes`
        A message payload containing S3 notifications.

    Yields
    ------
    oid : `str`
        The filename referred to by each message.
    """
    msg = json.loads(payload)
    for record in msg["Records"]:
        if not record["eventName"].startswith("ObjectCreated"):
            _log.warning("Unexpected non-creation notification in topic: %s", record)
            continue
        try:
            key = record["s3"]["object"]["key"]
            if not key.endswith(".json"):
                yield key
        except KeyError as e:
            _log.error("Invalid S3 bucket notification: %s", e)


def _try_export(mwi: MiddlewareInterface, exposures: set[int], log: logging.Logger) -> bool:
    """Attempt to export pipeline products, logging any failure.

    This method is designed to be safely run from within exception handlers.

    Returns
    -------
    exported : `bool`
        `True` if the export was successful, `False` for a (possibly partial)
        failure.
    """
    try:
        mwi.export_outputs(exposures)
        return True
    except Exception:
        log.exception("Central repo export failed. Some output products may be lost.")
        return False


def next_visit_handler() -> tuple[str, int]:
    """A Flask view function for handling next-visit events.

    Like all Flask handlers, this function accepts input through the
    ``flask.request`` global rather than parameters.

    Returns
    -------
    message : `str`
        The HTTP response reason to return to the client.
    status : `int`
        The HTTP response status code to return to the client.
    """
    _log.info(f"Starting next_visit_handler for {flask.request}.")

    try:
        try:
            expected_visit = parse_next_visit(flask.request)
        except ValueError as e:
            _log.exception("Bad Request")
            return f"Bad Request: {e}", 400
        process_visit(expected_visit)
        return "Pipeline executed", 200
    except GracefulShutdownInterrupt:
        # Safety net to minimize chance of interrupt propagating out of the worker.
        # Ideally, this would be a Flask.errorhandler, but Flask ignores BaseExceptions.
        _log.error("Service interrupted. Shutting down *without* syncing to the central repo.")
        return "The worker was interrupted before it could complete the request. " \
               "Retrying the request may not be safe.", 500
    finally:
        # Want to know when the handler exited for any reason.
        _log.info("next_visit handling completed.")


@with_signal(signal.SIGHUP, _graceful_shutdown)
@with_signal(signal.SIGTERM, _graceful_shutdown)
def process_visit(expected_visit: FannedOutVisit):
    """Prepare and run a pipeline on a nextVisit message.

    This function should not make any assumptions about the execution framework
    for the Prompt Processing system; in particular, it should not assume it is
    running on a web server.

    Parameters
    ----------
    expected_visit : `activator.visit.FannedOutVisit`
        The visit to process.

    Raises
    ------
    activator.exception.GracefulShutdownInterrupt
        Raised if the process was terminated at an unexpected point.
        Terminations during preprocessing or processing are chained by
        `~activator.exception.NonRetriableError` or
        `~activator.exception.RetriableError`, depending on the program state
        at the time.
    activator.exception.IgnorableVisit
        Raised if the service is configured to not process ``expected_visit``.
    activator.exception.InvalidVisitError
        Raised if ``expected_visit`` is not processable.
    activator.exception.NonRetriableError
        Raised if external resources (such as the APDB or alert stream) may
        have been left in a state that makes it unsafe to retry failures. This
        exception is always chained to another exception representing the
        original error.
    activator.exception.RetriableError
        Raised if the conditions for NonRetriableError are not met, *and*
        processing failed in a way that is expected to be transient. This
        exception is always chained to another exception representing the
        original error.
    """
    with contextlib.ExitStack() as cleanups:
        consumer = _get_consumer()
        consumer.subscribe([bucket_topic])
        cleanups.callback(consumer.unsubscribe)
        _log.debug(f"Created subscription to '{bucket_topic}'")
        # Try to get a message right away to minimize race conditions
        startup_response = consumer.consume(num_messages=1, timeout=0.001)

        assert expected_visit.instrument == instrument_name, \
            f"Expected {instrument_name}, received {expected_visit.instrument}."
        if not main_pipelines.get_pipeline_files(expected_visit):
            raise IgnorableVisit(f"No pipeline configured for {expected_visit}.")

        log_factory = logging.getLogRecordFactory()
        with log_factory.add_context(group=expected_visit.groupId,
                                     survey=expected_visit.survey,
                                     detector=expected_visit.detector,
                                     ):
            try:
                expid_set = set()

                # Create a fresh MiddlewareInterface object to avoid accidental
                # "cross-talk" between different visits.
                mwi = MiddlewareInterface(_get_central_butler(),
                                          image_bucket,
                                          expected_visit,
                                          pre_pipelines,
                                          main_pipelines,
                                          skymap,
                                          _get_local_repo().name,
                                          _get_local_cache())
                # TODO: pipeline execution requires a clean run until DM-38041.
                cleanups.callback(mwi.clean_local_repo, expid_set)
                # Copy calibrations for this detector/visit
                mwi.prep_butler()

                # expected_visit.nimages == 0 means "not known in advance"; keep listening until timeout
                expected_snaps = expected_visit.nimages if expected_visit.nimages else 100
                # Heuristic: take the upcoming script's duration and multiply by 2 to
                # include the currently executing script, then add time to transfer
                # the last image.
                timeout = expected_visit.duration * 2 + image_timeout
                # Check to see if any snaps have already arrived
                for snap in range(expected_snaps):
                    oid = check_for_snap(
                        _get_storage_client(),
                        image_bucket,
                        raw_microservice,
                        expected_visit.instrument,
                        expected_visit.groupId,
                        snap,
                        expected_visit.detector,
                    )
                    if oid:
                        _log.debug("Found object %s already present", oid)
                        exp_id = mwi.ingest_image(oid)
                        expid_set.add(exp_id)

                _log.debug("Waiting for snaps...")
                start = time.time()
                while len(expid_set) < expected_snaps and time.time() - start < timeout:
                    if startup_response:
                        response = startup_response
                    else:
                        time_remaining = max(0.0, timeout - (time.time() - start))
                        response = consumer.consume(num_messages=1, timeout=time_remaining + 1.0)
                    end = time.time()
                    messages = _filter_messages(response)
                    response = []
                    if len(messages) == 0 and end - start < timeout and not startup_response:
                        _log.debug(f"Empty consume after {end - start}s.")
                        continue
                    startup_response = []

                    # Not all notifications are for this group/detector
                    for received in messages:
                        for oid in _parse_bucket_notifications(received.value()):
                            try:
                                if is_path_consistent(oid, expected_visit):
                                    _log.debug("Received %r", oid)
                                    group_id = get_group_id_from_oid(oid)
                                    if group_id == expected_visit.groupId:
                                        # Ingest the snap
                                        exp_id = mwi.ingest_image(oid)
                                        expid_set.add(exp_id)
                            except ValueError:
                                _log.error(f"Failed to match object id '{oid}'")
                        # Commits are per-group, so this can't interfere with other
                        # workers. This may wipe messages associated with a next_visit
                        # that will later be assigned to this worker, but those cases
                        # should be caught by the "already arrived" check.
                        consumer.commit(message=received)
                if len(expid_set) < expected_snaps:
                    _log.warning(f"Timed out waiting for image after receiving exposures {expid_set}.")
            except GracefulShutdownInterrupt as e:
                raise RetriableError("Processing interrupted before pipeline execution") from e

            if expid_set:
                with log_factory.add_context(exposures=expid_set):
                    # Got at least some snaps; run the pipeline.
                    # If this is only a partial set, the processed results may still be
                    # useful for quality purposes.
                    # If nimages == 0, any positive number of snaps is OK.
                    if len(expid_set) < expected_visit.nimages:
                        _log.warning(f"Processing {len(expid_set)} snaps, "
                                     f"expected {expected_visit.nimages}.")
                    _log.info("Running pipeline...")
                    try:
                        mwi.run_pipeline(expid_set)
                        try:
                            mwi.export_outputs(expid_set)
                        except Exception as e:
                            raise NonRetriableError("APDB and possibly alerts or central repo modified") \
                                from e
                    except RetriableError:
                        # Do not export, to leave room for the next attempt
                        raise
                    except Exception:
                        _try_export(mwi, expid_set, _log)
                        raise
            else:
                raise RuntimeError("Timed out waiting for images.")


def invalid_visit(e: InvalidVisitError) -> tuple[str, int]:
    _log.exception("Invalid visit")
    return f"Cannot process visit: {e}.", 422


def skip_visit(e: IgnorableVisit) -> tuple[str, int]:
    _log.info("Skipping visit: %s", e)
    return f"Skipping visit without processing: {e}.", 422


def request_retry(e: RetriableError):
    error = e.nested if e.nested else e
    _log.error("Processing failed but can be retried: ", exc_info=error)
    # Service unavailable is not quite right, but no better standard response
    response = flask.make_response(
        f"The server could not process the request, but it can be retried: {error}",
        503,
        {"Retry-After": 30})
    return response


def forbid_retry(e: NonRetriableError) -> tuple[str, int]:
    error = e.nested if e.nested else e
    _log.error("Processing failed: ", exc_info=error)
    return f"An error occurred during processing: {error}.\nThe system's state has " \
           "permanently changed, so this request should **NOT** be retried.", 500


def server_error(e: Exception) -> tuple[str, int]:
    _log.exception("An error occurred during a request.")
    return (
        f"""
    An internal error occurred: <pre>{e}</pre>
    See logs for full stacktrace.
    """,
        500,
    )


def main():
    # Knative deployments call `create_app()()` through Gunicorn.
    # Keda deployments invoke main.
    if platform == "knative":
        _log.info("starting standalone Flask app")
        app = create_app()
        app.run(host="127.0.0.1", port=8080, debug=True)
    # starts keda instance of the application
    elif platform == "keda":
        _log.info("starting keda instance")
        keda_start()
    else:
        _log.info("no platform defined")


if __name__ == "__main__":
    main()
