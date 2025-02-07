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

import dataclasses
import itertools
import json
import logging
import random
import re
import sys
import tempfile
import time

import astropy.time
import boto3
from botocore.handlers import validate_bucket_name

from lsst.resources import ResourcePath

from shared.raw import (
    IMSIM_REGEXP,
    LSST_REGEXP,
    OTHER_REGEXP,
    get_raw_path,
    _LSST_CAMERA_LIST,
    _DETECTOR_FROM_RS,
)
from shared.visit import FannedOutVisit, SummitVisit
# Need explicit tester for command-line execution
from tester.utils import (
    INSTRUMENTS,
    get_last_group,
    increment_group,
    make_exposure_id,
    replace_header_key,
    send_next_visit,
)


EXPOSURE_INTERVAL = 18
SLEW_INTERVAL = 2


logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.INFO)


def process_group(kafka_url, visit_infos, uploader):
    """Simulate the observation of a single on-sky pointing.

    Parameters
    ----------
    kafka_url : `str`
        The URL of the Kafka REST Proxy to send ``next_visit`` messages to.
    visit_infos : `set` [`activator.FannedOutVisit`]
        The visit-detector combinations to be observed; each object may
        represent multiple snaps. Assumed to represent a single group, and to
        share instrument, nimages, filters, and survey.
    uploader : callable [`activator.FannedOutVisit`, int]
        A callable that takes an exposure spec and a snap ID, and uploads the
        visit's data.
    """
    # Assume group/snaps is shared among all visit_infos
    for info in visit_infos:
        group = info.groupId
        n_snaps = info.nimages
        visit = SummitVisit(**info.get_bare_visit(),
                            private_sndStamp=info.private_sndStamp,
                            private_efdStamp=astropy.time.Time(info.private_sndStamp, format="unix_tai").unix,
                            )
        send_next_visit(kafka_url, group, {visit})
        break
    else:
        _log.info("No observations to make; aborting.")
        return

    _log.info(f"Slewing to group {group}")
    time.sleep(SLEW_INTERVAL)

    # TODO: need asynchronous code to handle next_visit delay correctly
    for snap in range(n_snaps):
        _log.info(f"Taking group: {group} snap: {snap}")
        time.sleep(EXPOSURE_INTERVAL)
        for info in visit_infos:
            _log.info(f"Uploading group: {info.groupId} snap: {snap} filters: {info.filters} "
                      f"detector: {info.detector}")
            uploader(info, snap)
            _log.info(f"Uploaded group: {info.groupId} snap: {snap} filters: {info.filters} "
                      f"detector: {info.detector}")


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} INSTRUMENT N_GROUPS")
        sys.exit(1)
    instrument = sys.argv[1]
    n_groups = int(sys.argv[2])

    date = time.strftime("%Y%m%d")

    kafka_url = "https://usdf-rsp-dev.slac.stanford.edu/sasquatch-rest-proxy/topics/test.next-visit"
    endpoint_url = "https://s3dfrgw.slac.stanford.edu"
    s3 = boto3.resource("s3", endpoint_url=endpoint_url)
    dest_bucket = s3.Bucket("rubin-pp-dev")
    dest_bucket.meta.client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)

    src_bucket = s3.Bucket("rubin-pp-dev-users")
    src_bucket.meta.client.meta.events.unregister("before-parameter-build.s3", validate_bucket_name)

    last_group = get_last_group(dest_bucket, instrument, date)
    new_group_base = increment_group(instrument, last_group, random.randrange(10, 19))
    _log.info(f"Last group {last_group}. New group base {new_group_base}")

    if instrument in _LSST_CAMERA_LIST:
        raw_pool = get_samples_lsst(src_bucket, instrument)
    else:
        raw_pool = get_samples_non_lsst(src_bucket, instrument)

    if raw_pool:
        _log.info(f"Observing real raw files from {instrument}.")
        upload_from_raws(kafka_url, instrument, raw_pool, src_bucket, dest_bucket,
                         n_groups, new_group_base)
    else:
        _log.error(f"No raw files found for {instrument}, aborting.")


def _add_to_raw_pool(raw_pool, snap_num, visit, blob):
    """Add a detector-snap to the raw pool for uploading.

    Parameters
    ----------
    raw_pool : mapping [`str`, mapping [`int`, mapping [`activator.FannedOutVisit`, `s3.ObjectSummary`]]]
        Available raws as a mapping from group IDs to a mapping of snap ID.
        The value of the innermost mapping is the observation metadata for
        each detector, and a Blob representing the image taken in that
        detector-snap.
    visit : `activator.visit.FannedOutVisit`
        The visit-detector combination to be added with this raw.
    snap_num : `int`
        The snap number for this raw.
    blob : `s3.ObjectSummary`
        The raw image for this detector-snap.
    """
    group = visit.groupId
    if group in raw_pool:
        snap_dict = raw_pool[group]
        if snap_num in snap_dict:
            _log.debug(f"New detector {visit.detector} added to snap {snap_num} of group {group}.")
            detector_dict = snap_dict[snap_num]
            detector_dict[visit] = blob
        else:
            _log.debug(f"New snap {snap_num} added to group {group}.")
            snap_dict[snap_num] = {visit: blob}
    else:
        _log.debug(f"New group {group} registered.")
        raw_pool[group] = {snap_num: {visit: blob}}


def get_samples_non_lsst(bucket, instrument):
    """Return any predefined raw exposures for a non-LSST instrument.

    The raws follows the non-LSST filename format as defined in activator/raw.py:
    instrument/detector/group/snap/expid/filter/*.fz

    Parameters
    ----------
    bucket : `S3.Bucket`
        The bucket in which to search for predefined raws.
    instrument : `str`
        The short name of the instrument to sample.

    Returns
    -------
    raws : mapping [`str`, mapping [`int`, mapping [`activator.FannedOutVisit`, `s3.ObjectSummary`]]]
        A mapping from group IDs to a mapping of snap ID. The value of the
        innermost mapping is the observation metadata for each detector,
        and a Blob representing the image taken in that detector-snap.
    """
    # TODO: set up a lookup-friendly class to represent the return value

    # TODO: replace this dict with something more scalable.
    hsc_metadata = {
        59126: {"ra": 149.28531249999997, "dec": 2.935002777777778, "rot": 270.0, "time": 1457332820.0},
        59134: {"ra": 149.45749166666664, "dec": 2.926961111111111, "rot": 270.0, "time": 1457333685.0},
        59138: {"ra": 149.45739166666664, "dec": 1.4269472222222224, "rot": 270.0, "time": 1457334125.0},
        59142: {"ra": 149.4992083333333, "dec": 2.8853, "rot": 270.0, "time": 1457334559.0},
        59150: {"ra": 149.96643749999996, "dec": 2.2202916666666668, "rot": 270.0, "time": 1457335427.0},
        59152: {"ra": 149.9247333333333, "dec": 2.1577777777777776, "rot": 270.0, "time": 1457335765.0},
        59154: {"ra": 150.22329166666663, "dec": 2.238341666666667, "rot": 270.0, "time": 1457336099.0},
        59156: {"ra": 150.26497083333334, "dec": 2.1966694444444443, "rot": 270.0, "time": 1457336431.0},
        59158: {"ra": 150.30668333333332, "dec": 2.2591888888888887, "rot": 270.0, "time": 1457336763.0},
        59160: {"ra": 150.18157499999998, "dec": 2.2800083333333334, "rot": 270.0, "time": 1457337098.0},
    }

    # The pre-made raw files are stored with the "unobserved" prefix
    blobs = bucket.objects.filter(Prefix=f"unobserved/{instrument}/")
    duration = float(EXPOSURE_INTERVAL + SLEW_INTERVAL)
    result = {}
    for blob in blobs:
        # Assume that the unobserved bucket uses the same filename scheme as
        # the observed bucket.
        snap = re.match(OTHER_REGEXP, blob.key)
        group = snap["group"]
        exp_id = int(snap["expid"])
        snap_num = int(snap["snap"])
        visit = FannedOutVisit(
            instrument=instrument,
            detector=snap["detector"],
            groupId=group,
            nimages=INSTRUMENTS[instrument].n_snaps,
            filters=snap["filter"],
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[hsc_metadata[exp_id]["ra"], hsc_metadata[exp_id]["dec"]],
            startTime=hsc_metadata[exp_id]["time"],
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=hsc_metadata[exp_id]["rot"],
            survey="SURVEY",
            # Fan-out uses salIndex to know which instrument and detector config to use.
            # The exp_id of this test dataset is coded into fan-out's pattern matching.
            salIndex=exp_id,
            scriptSalIndex=42,
            dome=FannedOutVisit.Dome.OPEN,
            duration=duration,
            totalCheckpoints=1,
            private_sndStamp=hsc_metadata[exp_id]["time"]-2*duration,
        )
        _log.debug(f"File {blob.key} parsed as snap {snap_num} of visit {visit}.")
        _add_to_raw_pool(result, snap_num, visit, blob)

    return result


def get_samples_lsst(bucket, instrument):
    """Return any predefined raw exposures for a LSST instrument.

    The raws follows the LSST filename convention.

    Parameters
    ----------
    bucket : `S3.Bucket`
        The bucket in which to search for predefined raws.
    instrument : `str`
        The short name of the instrument to sample.

    Returns
    -------
    raws : mapping [`str`, mapping [`int`, mapping [`activator.FannedOutVisit`, `s3.ObjectSummary`]]]
        A mapping from group IDs to a mapping of snap ID. The value of the
        innermost mapping is the observation metadata for each detector,
        and a Blob representing the image taken in that detector-snap.
    """
    # The pre-made raw files are stored with the "unobserved" prefix
    blobs = bucket.objects.filter(Prefix=f"unobserved/{instrument}/")
    duration = float(EXPOSURE_INTERVAL + SLEW_INTERVAL)
    result = {}
    for blob in blobs:
        # Assume that the unobserved bucket uses the same filename scheme as
        # the observed bucket.
        if instrument == "LSSTCam-imSim":
            m = re.match(IMSIM_REGEXP, blob.key)
        else:
            m = re.match(LSST_REGEXP, blob.key)
        if not m or m["extension"] == ".json":
            continue

        # Retrieve the corresponding sidecar json file
        sidecar = ResourcePath("s3://" + blob.bucket_name).join(
            blob.key.removesuffix(m["extension"]) + ".json"
        )
        if not sidecar.exists():
            raise RuntimeError(f"Unable to retrieve JSON sidecar: {sidecar}")
        with sidecar.open("r") as f:
            md = json.load(f)

        visit = FannedOutVisit(
            instrument=instrument,
            detector=_DETECTOR_FROM_RS[instrument][m["raft_sensor"]],
            groupId=md["GROUPID"],
            nimages=INSTRUMENTS[instrument].n_snaps,
            # ComCam currently sets the FILTBAND header to null.
            filters=md["FILTBAND"] or md["FILTER"],
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[md["RA"], md["DEC"]],
            startTime=astropy.time.Time(md["DATE-BEG"], format="isot", scale="tai").unix_tai,
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=md["ROTPA"],
            survey="SURVEY",
            salIndex=INSTRUMENTS[instrument].sal_index,
            scriptSalIndex=2,
            dome=FannedOutVisit.Dome.OPEN,
            duration=duration,
            totalCheckpoints=1,
            private_sndStamp=astropy.time.Time(md["DATE-BEG"], format="isot", scale="tai"
                                               ).unix_tai-2*duration,
        )
        _log.debug(f"File {blob.key} parsed as visit {visit} and registered as group {md['GROUPID']}.")
        _add_to_raw_pool(result, 0, visit, blob)

    return result


def upload_from_raws(kafka_url, instrument, raw_pool, src_bucket, dest_bucket, n_groups, group_base):
    """Upload visits and files using real raws.

    Parameters
    ----------
    kafka_url : `str`
        The URL of the Kafka REST Proxy to send ``next_visit`` messages to.
    instrument : `str`
        The short name of the instrument carrying out the observation.
    raw_pool : mapping [`str`, mapping [`int`, mapping [`activator.FannedOutVisit`, `s3.ObjectSummary`]]]
        Available raws as a mapping from group IDs to a mapping of snap ID.
        The value of the innermost mapping is the observation metadata for
        each detector, and a Blob representing the image taken in that
        detector-snap.
    src_bucket : `S3.Bucket`
        The bucket containing the blobs in ``raw_pool``.
    dest_bucket : `S3.Bucket`
        The bucket to which to upload the new images.
    n_groups : `int`
        The number of observation groups to simulate. If more than the number
        of groups in ``raw_pool``, files will be re-uploaded under new
        group IDs.
    group_base : `str`
        The base group ID from which to offset new group IDs.

    Exceptions
    ----------
    ValueError
        Raised if ``n_groups`` exceeds the number of groups in ``raw_pool``.
    """
    if n_groups > len(raw_pool):
        raise ValueError(f"Requested {n_groups} groups, but only {len(raw_pool)} "
                         "unobserved raw groups are available. "
                         "For large-scale tests, consider upload_from_repo.py.")

    for i, true_group in enumerate(itertools.islice(raw_pool, n_groups)):
        group = increment_group(instrument, group_base, i)
        _log.info(f"Processing group {group} from unobserved {true_group}...")
        # snap_dict maps snap_id to {visit: blob}
        snap_dict = {}
        # Copy all the visit-blob dictionaries under each snap_id,
        # replacing the (immutable) FannedOutVisit objects to point to group
        # instead of true_group.
        for snap_id, old_visits in raw_pool[true_group].items():
            snap_dict[snap_id] = {dataclasses.replace(true_visit, groupId=group): blob
                                  for true_visit, blob in old_visits.items()}
        # Gather all the FannedOutVisit objects found in snap_dict, merging
        # duplicates for different snaps of the same detector.
        visit_infos = {info for det_dict in snap_dict.values() for info in det_dict}

        # TODO: may be cleaner to use a functor object than to depend on
        # closures for the buckets and data.
        def upload_from_pool(visit, snap_id):
            src_blob = snap_dict[snap_id][visit]
            exposure_num, headers = \
                make_exposure_id(visit.instrument, visit.groupId, snap_id)
            filename = get_raw_path(visit.instrument, visit.detector, visit.groupId, snap_id,
                                    exposure_num, visit.filters)

            if instrument in _LSST_CAMERA_LIST:
                # Upload a corresponding sidecar json file
                sidecar = ResourcePath("s3://" + src_blob.bucket_name).join(
                    src_blob.key.removesuffix("fits") + "json"
                )
                filename_sidecar = filename.removesuffix("fits") + "json"
                with sidecar.open("r") as f:
                    md = json.load(f)
                    md.update(headers)
                    dest_bucket.put_object(Body=json.dumps(md), Key=filename_sidecar)

            # r+b required by replace_header_key.
            with tempfile.TemporaryFile(mode="r+b") as buffer:
                src_bucket.download_fileobj(src_blob.key, buffer)
                for header_key in headers:
                    replace_header_key(buffer, header_key, headers[header_key])
                buffer.seek(0)  # Assumed by upload_fileobj.
                dest_bucket.upload_fileobj(buffer, filename)
            _log.debug(f"{filename} is uploaded to {dest_bucket}")

        process_group(kafka_url, visit_infos, upload_from_pool)


if __name__ == "__main__":
    main()
