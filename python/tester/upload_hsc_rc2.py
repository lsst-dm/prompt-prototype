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

import logging
import os
import random
import sys
import tempfile
import time

import boto3

from lsst.daf.butler import Butler

from activator.raw import get_raw_path
from activator.visit import SummitVisit
from tester.utils import get_last_group, make_exposure_id, replace_header_key, send_next_visit


EXPOSURE_INTERVAL = 18
SLEW_INTERVAL = 2

# Kafka server
kafka_cluster = os.environ["KAFKA_CLUSTER"]

logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.INFO)


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} N_GROUPS")
        sys.exit(1)
    n_groups = int(sys.argv[1])

    date = time.strftime("%Y%m%d")

    kafka_url = "https://usdf-rsp-dev.slac.stanford.edu/sasquatch-rest-proxy/topics/test.next-visit"
    endpoint_url = "https://s3dfrgw.slac.stanford.edu"
    s3 = boto3.resource("s3", endpoint_url=endpoint_url)
    dest_bucket = s3.Bucket("rubin-pp")

    last_group = get_last_group(dest_bucket, "HSC", date)
    group_num = last_group + random.randrange(10, 19)
    _log.debug(f"Last group {last_group}; new group base {group_num}")

    butler = Butler("/repo/main")
    visit_list = get_hsc_visit_list(butler, n_groups)
    for visit in visit_list:
        group_num += 1
        _log.info(f"Slewing to group {group_num}, with HSC visit {visit}")
        time.sleep(SLEW_INTERVAL)
        refs = prepare_one_visit(kafka_url, str(group_num), butler, visit)
        _log.info(f"Taking exposure for group {group_num}")
        time.sleep(EXPOSURE_INTERVAL)
        _log.info(f"Uploading detector images for group {group_num}")
        upload_hsc_images(dest_bucket, str(group_num), butler, refs)


def get_hsc_visit_list(butler, n_sample):
    """Return a list of randomly selected raw visits from HSC-RC2 in the butler repo.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The Butler in which to search for records of raw data.
    n_sample: `int`
        The number of visits to select.

    Returns
    -------
    visits : `list` [`int`]
        A list of randomly selected visit IDs from the HSC-RC2 dataset
    """
    results = butler.registry.queryDimensionRecords(
        "visit",
        datasets="raw",
        collections="HSC/RC2/defaults",
        where="instrument='HSC' and exposure.observation_type='science'",
    )
    rc2 = [record.id for record in set(results)]
    if n_sample > len(rc2):
        raise ValueError(f"Requested {n_sample} groups, but only {len(rc2)} are available.")
    visits = random.sample(rc2, k=n_sample)
    return visits


def prepare_one_visit(kafka_url, group_id, butler, visit_id):
    """Extract metadata and send next_visit events for one HSC-RC2 visit

    One ``next_visit`` message is sent for each detector, to mimic the
    current prototype design in which a single message is sent from the
    Summit to the USDF and then a USDF-based server translates it into
    multiple messages.

    Parameters
    ----------
    kafka_url : `str`
        The URL of the Kafka REST Proxy to send ``next_visit`` messages to.
    group_id : `str`
        The group ID for the message to send.
    butler : `lsst.daf.butler.Butler`
        The Butler with the raw data.
    visit_id : `int`
        The ID of a visit in the HSC-RC2 dataset.

    Returns
    -------
    refs : iterable of `lsst.daf.butler.DatasetRef`
        The datasets for which the events are sent.
    """
    refs = butler.registry.queryDatasets(
        datasetType="raw",
        collections="HSC/RC2/defaults",
        dataId={"exposure": visit_id, "instrument": "HSC"},
    )

    # all items in refs share the same visit info and one event is to be sent
    for data_id in refs.dataIds.limit(1).expanded():
        visit = SummitVisit(
            groupId=group_id,
            nimages=1,
            filters=data_id.records["physical_filter"].name,
            coordinateSystem=SummitVisit.CoordSys.ICRS,
            position=[data_id.records["exposure"].tracking_ra, data_id.records["exposure"].tracking_dec],
            rotationSystem=SummitVisit.RotSys.SKY,
            cameraAngle=data_id.records["exposure"].sky_angle,
            survey="SURVEY",
            salIndex=999,
            scriptSalIndex=999,
            dome=SummitVisit.Dome.OPEN,
            duration=float(EXPOSURE_INTERVAL+SLEW_INTERVAL),
            totalCheckpoints=1,
        )
        send_next_visit(kafka_url, group_id, {visit})

    return refs


def upload_hsc_images(dest_bucket, group_id, butler, refs):
    """Upload one group of raw HSC images to the central repo

    Parameters
    ----------
    dest_bucket: `S3.Bucket`
        The bucket to which to upload the images.
    group_id : `str`
        The group ID under which to store the images.
    butler : `lsst.daf.butler.Butler`
        The source Butler with the raw data.
    refs : iterable of `lsst.daf.butler.DatasetRef`
        The datasets to upload
    """
    exposure_key, exposure_header, exposure_num = make_exposure_id("HSC", int(group_id), 0)
    with tempfile.TemporaryDirectory() as temp_dir:
        # Each ref is done separately because butler.retrieveArtifacts does not preserve the order.
        for ref in refs:
            dest_key = get_raw_path(
                "HSC",
                ref.dataId["detector"],
                group_id,
                0,
                exposure_num,
                ref.dataId["physical_filter"],
            )
            transferred = butler.retrieveArtifacts(
                [ref],
                transfer="copy",
                preserve_path=False,
                destination=temp_dir,
            )
            if len(transferred) != 1:
                _log.error(
                    f"{ref} has multitple artifacts and cannot be handled by current implementation"
                )
                continue

            path = transferred[0].path
            _log.debug(
                f"Raw file for {ref.dataId} was copied from Butler to {path}"
            )
            with open(path, "r+b") as temp_file:
                replace_header_key(temp_file, exposure_key, exposure_header)
            dest_bucket.upload_file(path, dest_key)
            _log.debug(f"{dest_key} was written at {dest_bucket}")


if __name__ == "__main__":
    main()
