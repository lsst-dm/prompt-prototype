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

"""Common definitions of raw paths.

This module provides tools to convert raw paths into exposure metadata and
vice versa.
"""

__all__ = [
    "is_path_consistent",
    "check_for_snap",
    "get_prefix_from_snap",
    "get_exp_id_from_oid",
    "get_group_id_from_oid",
    "LSST_REGEXP",
    "IMSIM_REGEXP",
    "OTHER_REGEXP",
    "get_raw_path",
]

import json
import logging
import os
import re
import time
import urllib.parse

import requests

from lsst.obs.lsst import LsstCam, LsstCamImSim, LsstComCam, LsstComCamSim
from lsst.obs.lsst.translators.lsst import LsstBaseTranslator
from lsst.resources import ResourcePath

from .visit import FannedOutVisit

_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)

# Format for filenames of LSST camera raws uploaded to image bucket:
# instrument/dayobs/obsid/obsid_Rraft_Ssensor.(fits, fz, fits.gz)
LSST_REGEXP = re.compile(
    r"(?P<instrument>.*?)/(?P<day_obs>\d+)/(?P<obs_id>.*?)/"
    r"(?P=obs_id)_(?P<raft_sensor>R\d\d_S.\d)(?P<extension>\.f.*)$"
)

# Format for LSSTCam-imSim raws uploaded to image bucket:
# instrument/day_obs/expid/expid_filter_detector.fits
IMSIM_REGEXP = re.compile(
    r"(?P<instrument>.*?)/(?P<day_obs>\d+)/(?P<expid>.*?)/"
    r"(?P=expid)_(?P<filter>.*?)_(?P<detector>\d+)(?P<extension>\.f.*)$"
)

# Format for filenames of non-LSST camera raws uploaded to image bucket:
# instrument/detector/group/snap/expid/filter/*.(fits, fz, fits.gz)
OTHER_REGEXP = re.compile(
    r"(?P<instrument>.*?)/(?P<detector>\d+)/(?P<group>.*?)/(?P<snap>\d+)/(?P<expid>.*?)/(?P<filter>.*?)/"
    r"[^/]+\.f"
)

################################
# LSST Specific Initialization #
################################

# The list of camera names that might be used for LSST
_LSST_CAMERA_LIST = (
    "LATISS",
    "ComCam",
    "LSSTComCam",
    "LSSTComCamSim",
    "LSSTCam",
    "TS8",
    "LSST-TS8",
    "LSSTCam-imSim",
)

# Translate from Camera path prefixes to official names.
_TRANSLATE_INSTRUMENT = {
    "ComCam": "LSSTComCam",
    "TS8": "LSST-TS8",
}

# Abbreviations for cameras.
_CAMERA_ABBREV = {
    "LATISS": "AT",
    "LSSTComCam": "CC",
    "LSSTComCamSim": "CC",
    "LSSTCam": "MC",
    "LSST-TS8": "TS",
}

# For each LSST Camera, we need the mapping from detector name to detector
# number and back.  LATISS and LSST-TS8 are handled specially due to
# inconsistencies between the obs_lsst CameraGeom and the Camera Control
# System outputs.
#
# Note that the getCamera() method may be replaced in the future.

_LSSTCAM = LsstCam.getCamera().getNameMap()
_LSSTCAMIMSIM = LsstCamImSim.getCamera().getNameMap()
_LSSTCOMCAM = LsstComCam.getCamera().getNameMap()
_LSSTCOMCAMSIM = LsstComCamSim.getCamera().getNameMap()

_DETECTOR_FROM_RS = {
    "LATISS": {"R00_S00": 0},
    "LSSTComCam": {name: value.getId() for name, value in _LSSTCOMCAM.items()},
    "LSSTComCamSim": {name: value.getId() for name, value in _LSSTCOMCAMSIM.items()},
    "LSST-TS8": {f"R22_S{x}{y}": x * 3 + y for x in range(3) for y in range(3)},
    "LSSTCam": {name: value.getId() for name, value in _LSSTCAM.items()},
    "LSSTCam-imSim": {name: value.getId() for name, value in _LSSTCAMIMSIM.items()},
}

# Build the reverse mapping.
_DETECTOR_FROM_INT = {
    instrument: {
        detector: raft_sensor
        for raft_sensor, detector in camera.items()
    }
    for instrument, camera in _DETECTOR_FROM_RS.items()
}

###############################################################################


def is_path_consistent(oid: str, visit: FannedOutVisit) -> bool:
    """Test if this snap could have come from a particular visit.

    Parameters
    ----------
    oid : `str`
        The object store path to the snap image.
    visit : `activator.visit.FannedOutVisit`
        The visit from which snaps were expected.

    Returns
    -------
    consistent: `bool`
        True if the snap matches the visit as far as can be determined.
    """
    instrument, _ = oid.split("/", maxsplit=1)
    if instrument not in _LSST_CAMERA_LIST:
        m = re.match(OTHER_REGEXP, oid)
        if m:
            return (
                m["instrument"] == visit.instrument
                and int(m["detector"]) == visit.detector
                and m["group"] == visit.groupId
                # nimages == 0 means there can be any number of snaps
                and (int(m["snap"]) < visit.nimages or visit.nimages == 0)
            )
    elif instrument == "LSSTCam-imSim":
        m = re.match(IMSIM_REGEXP, oid)
        if m:
            return (
                m["instrument"] == visit.instrument
                and int(m["detector"]) == visit.detector
            )
    else:
        instrument = _TRANSLATE_INSTRUMENT.get(instrument, instrument)
        m = re.match(LSST_REGEXP, oid)
        if m:
            detector = _DETECTOR_FROM_RS[instrument][m["raft_sensor"]]
            return instrument == visit.instrument and detector == visit.detector

    return False


def check_for_snap(
    client,
    bucket: str,
    microservice: str,
    instrument: str,
    group: int,
    snap: int,
    detector: int,
) -> str | None:
    """Search for new raw files matching a particular data ID.

    The search is performed in the active image bucket or in a raw
    image microservice, if one is available.

    Parameters
    ----------
    client : `S3.Client`
        The client object with which to do the search.
    bucket : `str`
        The name of the bucket in which to search.
    microservice : `str`
        The URI of an optional microservice to assist the search.
    instrument, group, snap, detector
        The data ID to search for.

    Returns
    -------
    name : `str` or `None`
        The raw's object key within ``bucket``, or `None` if no file
        was found. If multiple files match, this function logs an error
        but returns one of the files anyway.
    """
    if microservice:
        try:
            return _query_microservice(microservice=microservice,
                                       instrument=instrument,
                                       group=group,
                                       detector=detector,
                                       snap=snap,
                                       )
        except RuntimeError:
            _log.exception("Could not query microservice, falling back to prefix algorithm.")

    prefix = get_prefix_from_snap(instrument, group, detector, snap)
    if not prefix:
        return None
    _log.debug(f"Checking for '{prefix}'")
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if response["KeyCount"] == 0:
        return None
    elif response["KeyCount"] > 1:
        _log.error(
            f"Multiple files detected for a single detector/group/snap: '{prefix}'"
        )
    # Contents only exists if >0 objects found.
    return response["Contents"][0]['Key']


def get_prefix_from_snap(
    instrument: str, group: str, detector: int, snap: int
) -> str | None:
    """Compute path prefix for a raw image object from a data id.

    Parameters
    ----------
    instrument : `str`
        The name of the instrument taking the image.
    group : `str`
        The group id from the visit, associating the snaps making up the visit.
    detector : `int`
        The integer detector id for the image being sought.
    snap : `int`
        The snap number within the group for the visit.

    Returns
    -------
    prefix : `str` or `None`
        The prefix to a path to the corresponding raw image object.  If it
        can be calculated, then the prefix may be the entire path.  If no
        prefix can be calculated, `None` is returned.
    """
    if instrument not in _LSST_CAMERA_LIST:
        return f"{instrument}/{detector}/{group}/{snap}/"
    return None


def _query_microservice(
    microservice: str, instrument: str, group: str, detector: int, snap: int
) -> str | None:
    """Look up a raw image's location from the raw image microservice.

    Parameters
    ----------
    microservice : `str`
        The URI of the microservice to query.
    instrument : `str`
        The name of the instrument taking the image.
    group : `str`
        The group id from the visit, associating the snaps making up the visit.
    detector : `int`
        The integer detector id for the image being sought.
    snap : `int`
        The snap number within the group for the visit.

    Returns
    -------
    key : `str` or `None`
        The raw's object key within its bucket, or `None` if no image was found.

    Raises
    ------
    RuntimeError
        Raised if this function could not connect to the microservice, or if the
        microservice encountered an error.
    """
    detector_name = _DETECTOR_FROM_INT[instrument][detector]
    uri = f"{microservice}/{instrument}/{group}/{snap}/{detector_name}"
    _log.debug("Querying %s for raws...", uri)
    try:
        response = requests.get(uri, timeout=1.0)
        response.raise_for_status()
        unpacked = response.json()
    except requests.Timeout as e:
        raise RuntimeError("Timed out connecting to raw microservice.") from e
    except requests.RequestException as e:
        raise RuntimeError("Could not query raw microservice.") from e

    _log.debug("Microservice sent: %s", unpacked)
    if unpacked["error"]:
        raise RuntimeError(f"Raw microservice had an internal error: {unpacked['message']}")
    if unpacked["present"]:
        # Need to return just the key, without the bucket
        path = urllib.parse.urlparse(unpacked["uri"], allow_fragments=False).path
        # Valid key does not start with a /
        return path.lstrip("/")
    else:
        return None


def get_exp_id_from_oid(oid: str) -> int:
    """Calculate an exposure id from an image object's pathname.

    Parameters
    ----------
    oid : `str`
        A pathname to an image object.

    Returns
    -------
    exp_id: `int`
        The exposure identifier as an integer.
    """
    instrument, _ = oid.split("/", maxsplit=1)
    if instrument not in _LSST_CAMERA_LIST:
        m = re.match(OTHER_REGEXP, oid)
        if m:
            return int(m["expid"])
    elif instrument == "LSSTCam-imSim":
        m = re.match(IMSIM_REGEXP, oid)
        if m:
            return int(m["expid"])
    else:
        instrument = _TRANSLATE_INSTRUMENT.get(instrument, instrument)
        m = re.match(LSST_REGEXP, oid)
        if m:
            # Ignore instrument abbreviation and controller
            _, _, day_obs, seq_num = m["obs_id"].split("_")
            return LsstBaseTranslator.compute_exposure_id(day_obs, int(seq_num))

    raise ValueError(f"{oid} could not be parsed into an exp_id")


def get_group_id_from_oid(oid: str) -> str:
    """Calculate a group id from an image object's pathname.

    This is more complex for LSST cameras because the information is not
    extractable from the oid.  Instead, we have to look at a "sidecar JSON"
    file to retrieve the group id.

    Parameters
    ----------
    oid : `str`
        A pathname to an image object.

    Returns
    -------
    group_id: `str`
        The group identifier as a string.
    """
    instrument, _ = oid.split("/", maxsplit=1)
    if instrument not in _LSST_CAMERA_LIST:
        m = re.match(OTHER_REGEXP, oid)
        if m:
            return m["group"]
        raise ValueError(f"{oid} could not be parsed into a group")

    if instrument == "LSSTCam-imSim":
        m = re.match(IMSIM_REGEXP, oid)
    else:
        m = re.match(LSST_REGEXP, oid)
    if not m:
        raise ValueError(f"{oid} could not be parsed.")
    sidecar = ResourcePath("s3://" + os.environ["IMAGE_BUCKET"]).join(
        # Can't use updatedExtension because we may have something like .fits.fz
        oid.removesuffix(m["extension"])
        + ".json"
    )
    # Wait a bit but not too long for the file.
    # It should normally show up before the image.
    count = 0
    while not sidecar.exists():
        count += 1
        if count > 20:
            raise RuntimeError(f"Unable to retrieve JSON sidecar: {sidecar}")
        time.sleep(0.1)

    with sidecar.open("r") as f:
        md = json.load(f)

    return md.get("GROUPID", "")


def get_raw_path(instrument, detector, group, snap, exposure_id, filter):
    """The path on which to store raws in the image bucket."""
    if instrument not in _LSST_CAMERA_LIST:
        return (
            f"{instrument}/{detector}/{group}/{snap}/{exposure_id}/{filter}"
            f"/{instrument}-{group}-{snap}"
            f"-{exposure_id}-{filter}-{detector}.fz"
        )

    # imSim raw data do not follow LSST convention and use OBSID differently.
    # For PP testing, we make up a file path convention, with the assumption
    # of group_id in the format YYYY-MM-DDTHH:MM:SS.ffffff like LSST.
    if instrument == "LSSTCam-imSim":
        day_obs = group[:10].replace("-", "")
        return (
            f"{instrument}/{day_obs}/{exposure_id}/"
            f"{exposure_id}_{filter}_{detector}.fits"
        )

    day_obs, seq_num, controller = LsstBaseTranslator.unpack_exposure_id(exposure_id)
    abbrev = _CAMERA_ABBREV[instrument]
    raft_sensor = _DETECTOR_FROM_INT[instrument][detector]
    obs_id = f"{abbrev}_{controller}_{day_obs}_{seq_num:06d}"
    return f"{instrument}/{day_obs}/{obs_id}/{obs_id}_{raft_sensor}.fits"
