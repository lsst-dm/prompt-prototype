# This file is part of prompt_prototype.
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

__all__ = ["check_for_snap", "next_visit_handler"]

import base64
import json
import logging
import os
import re
import time
from typing import Optional, Tuple

from flask import Flask, request
from google.cloud import pubsub_v1, storage

from lsst.daf.butler import Butler
from lsst.obs.base import Instrument
from .logger import GCloudStructuredLogFormatter
from .make_pgpass import make_pgpass
from .middleware_interface import MiddlewareInterface
from .raw import RAW_REGEXP
from .visit import Visit

PROJECT_ID = "prompt-proto"

verification_token = os.environ["PUBSUB_VERIFICATION_TOKEN"]
# The full instrument class name, including module path.
config_instrument = os.environ["RUBIN_INSTRUMENT"]
active_instrument = Instrument.from_string(config_instrument)
calib_repo = os.environ["CALIB_REPO"]
image_bucket = os.environ["IMAGE_BUCKET"]
timeout = os.environ.get("IMAGE_TIMEOUT", 50)

# Set up logging for all modules used by this worker.
log_handler = logging.StreamHandler()
log_handler.setFormatter(GCloudStructuredLogFormatter(
    labels={"instrument": active_instrument.getName()},
))
logging.basicConfig(handlers=[log_handler])
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
logging.captureWarnings(True)


# Write PostgreSQL credentials.
# This MUST be done before creating a Butler or accessing the APDB.
make_pgpass()


app = Flask(__name__)

subscriber = pubsub_v1.SubscriberClient()
topic_path = subscriber.topic_path(
    PROJECT_ID,
    f"{active_instrument.getName()}-image",
)
subscription = None

storage_client = storage.Client()

# Initialize middleware interface.
# TODO: this should not be done in activator.py, which is supposed to have only
# framework/messaging support (ideally, it should not contain any LSST imports).
# However, we don't want MiddlewareInterface to need to know details like where
# the central repo is located, either, so perhaps we need a new module.
central_butler = Butler(calib_repo,
                        collections=[active_instrument.makeCollectionName("defaults")],
                        writeable=False,
                        inferDefaults=False)
repo = f"/tmp/butler-{os.getpid()}"
butler = Butler(Butler.makeRepo(repo), writeable=True)
_log.info("Created local Butler repo at %s.", repo)
mwi = MiddlewareInterface(central_butler, image_bucket, config_instrument, butler)


def check_for_snap(
    instrument: str, group: int, snap: int, detector: int
) -> Optional[str]:
    """Search for new raw files matching a particular data ID.

    The search is performed in the active image bucket.

    Parameters
    ----------
    instrument, group, snap, detector
        The data ID to search for.

    Returns
    -------
    name : `str` or `None`
        The raw's location in the active bucket, or `None` if no file
        was found. If multiple files match, this function logs an error
        but returns one of the files anyway.
    """
    prefix = f"{instrument}/{detector}/{group}/{snap}/"
    _log.debug(f"Checking for '{prefix}'")
    blobs = list(storage_client.list_blobs(image_bucket, prefix=prefix))
    if not blobs:
        return None
    elif len(blobs) > 1:
        _log.error(
            f"Multiple files detected for a single detector/group/snap: '{prefix}'"
        )
    return blobs[0].name


@app.route("/next-visit", methods=["POST"])
def next_visit_handler() -> Tuple[str, int]:
    """A Flask view function for handling next-visit events.

    Like all Flask handlers, this function accepts input through the
    ``request`` global rather than parameters.

    Returns
    -------
    message : `str`
        The HTTP response reason to return to the client.
    status : `int`
        The HTTP response status code to return to the client.
    """
    if request.args.get("token", "") != verification_token:
        return "Invalid request", 400
    subscription = subscriber.create_subscription(
        topic=topic_path,
        ack_deadline_seconds=60,
    )
    _log.debug(f"Created subscription '{subscription.name}'")
    try:
        envelope = request.get_json()
        if not envelope:
            msg = "no Pub/Sub message received"
            _log.warn(f"error: '{msg}'")
            return f"Bad Request: {msg}", 400

        if not isinstance(envelope, dict) or "message" not in envelope:
            msg = "invalid Pub/Sub message format"
            _log.warn(f"error: '{msg}'")
            return f"Bad Request: {msg}", 400

        payload = base64.b64decode(envelope["message"]["data"])
        data = json.loads(payload)
        expected_visit = Visit(**data)
        assert expected_visit.instrument == active_instrument.getName(), \
            f"Expected {active_instrument.getName()}, received {expected_visit.instrument}."
        expid_set = set()

        # Copy calibrations for this detector/visit
        mwi.prep_butler(expected_visit)

        # Check to see if any snaps have already arrived
        for snap in range(expected_visit.snaps):
            oid = check_for_snap(
                expected_visit.instrument,
                expected_visit.group,
                snap,
                expected_visit.detector,
            )
            if oid:
                m = re.match(RAW_REGEXP, oid)
                mwi.ingest_image(oid)
                expid_set.add(m.group('expid'))

        _log.debug(f"Waiting for snaps from {expected_visit}.")
        start = time.time()
        while len(expid_set) < expected_visit.snaps:
            response = subscriber.pull(
                subscription=subscription.name,
                max_messages=189 + 8 + 8,
                timeout=timeout,
            )
            end = time.time()
            if len(response.received_messages) == 0:
                if end - start < timeout:
                    _log.debug(f"Empty pull after {end - start}s for {expected_visit}.")
                    continue
                _log.warning(
                    f"Timed out waiting for image in {expected_visit} "
                    f"after receiving exposures {expid_set}"
                )
                break

            ack_list = []
            for received in response.received_messages:
                ack_list.append(received.ack_id)
                oid = received.message.attributes["objectId"]
                m = re.match(RAW_REGEXP, oid)
                if m:
                    instrument, detector, group, snap, expid = m.groups()
                    _log.debug("instrument, detector, group, snap, expid = %s", m.groups())
                    if (
                        instrument == expected_visit.instrument
                        and int(detector) == int(expected_visit.detector)
                        and group == str(expected_visit.group)
                        and int(snap) < int(expected_visit.snaps)
                    ):
                        # Ingest the snap
                        mwi.ingest_image(oid)
                        expid_set.add(expid)
                else:
                    _log.error(f"Failed to match object id '{oid}'")
            subscriber.acknowledge(subscription=subscription.name, ack_ids=ack_list)

        if expid_set:
            # Got at least some snaps; run the pipeline.
            # If this is only a partial set, the processed results may still be
            # useful for quality purposes.
            if len(expid_set) < expected_visit.snaps:
                _log.warning(f"Processing {len(expid_set)} snaps, expected {expected_visit.snaps}.")
            _log.info(f"Running pipeline on {expected_visit}.")
            mwi.run_pipeline(expected_visit, expid_set)
            return "Pipeline executed", 200
        else:
            _log.fatal(f"Timed out waiting for images for {expected_visit}.")
            return "Timed out waiting for images", 500
    finally:
        subscriber.delete_subscription(subscription=subscription.name)


@app.errorhandler(500)
def server_error(e) -> Tuple[str, int]:
    _log.exception("An error occurred during a request.")
    return (
        f"""
    An internal error occurred: <pre>{e}</pre>
    See logs for full stacktrace.
    """,
        500,
    )


def main():
    with subscriber:
        app.run(host="127.0.0.1", port=8080, debug=True)


if __name__ == "__main__":
    main()
