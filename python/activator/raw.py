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

"""Common definitions of raw paths.

This module provides tools to convert raw paths into exposure metadata and
vice versa.
"""

__all__ = [
    "is_path_consistent",
    "get_prefix_from_snap",
    "get_exp_id_from_oid",
    "get_group_id_from_oid",
    "LSST_REGEXP",
    "OTHER_REGEXP",
    "get_raw_path",
]

import json
import os
import re
import time

from lsst.resources import ResourcePath

from .visit import FannedOutVisit

# Format for filenames of LSST camera raws uploaded to image bucket:
# instrument/dayobs/obsid/obsid_Rraft_Ssensor.(fits, fz, fits.gz)
LSST_REGEXP = re.compile(
    r"(?P<instrument>.*?)/(?P<day_obs>\d+)/(?P<obs_id>.*?)/"
    r"(?P=obs_id)_(?P<raft_sensor>R\d\d_S.\d)(?P<extension>\.f.*)$"
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
_LSST_CAMERA_LIST = ("LATISS", "ComCam", "LSSTComCam", "LSSTCam", "TS8", "LSST-TS8")

# Translate from Camera path prefixes to official names.
_TRANSLATE_INSTRUMENT = {
    "ComCam": "LSSTComCam",
    "TS8": "LSST-TS8",
}

# Abbreviations for cameras.
_CAMERA_ABBREV = {
    "LATISS": "AT",
    "LSSTComCam": "CC",
    "LSSTCam": "MC",
    "LSST-TS8": "TS",
}

# For each LSST Camera, we need the mapping from detector name to detector
# number and back.  This is officially in obs_lsst, but coding it here is
# simpler than retrieving a Camera object, most of the contents of which we
# don't need.

# First, define the starting point (S00) of each science raft of LSSTCam
# (these are multiplied by 9 CCDs per raft later).
_LSSTCAM_RAFTS = {
    "R01": 0, "R02": 1, "R03": 2,
    "R10": 3, "R11": 4, "R12": 5, "R13": 6, "R14": 7,
    "R20": 8, "R21": 9, "R22": 10, "R23": 11, "R24": 12,
    "R30": 13, "R31": 14, "R32": 15, "R33": 16, "R34": 17,
    "R41": 18, "R42": 19, "R43": 20,
}
# Then add the "S00" sensor for each corner raft (not multiplied).
_LSSTCAM_CORNER_RAFTS = {
    "R00": 189, "R04": 193, "R40": 197, "R44": 201
}

# Then build the camera sensor translation maps.  LATISS has only one sensor.
# LSSTComCam and LSST-TS8 have 1 raft, named "R22".  LSSTCam has the full set.
# Sensor numbers start at the raft starting point and increase in the y
# (second digit) direction before the x (first digit) direction.

_DETECTOR_FROM_RS = {
    "LATISS": {"R00_S00": 0},
    "LSSTComCam": {f"R22_S{x}{y}": x * 3 + y for x in range(3) for y in range(3)},
    "LSST-TS8": {f"R22_S{x}{y}": x * 3 + y for x in range(3) for y in range(3)},
    "LSSTCam": {
        f"{raft}_S{x}{y}": start * 9 + x * 3 + y
        for raft, start in _LSSTCAM_RAFTS.items()
        for x in range(3)
        for y in range(3)
    }
}
# Add in the corner rafts for LSSTCam only.  These have special naming
# conventions for Guiders and Wavefront sensors.
_DETECTOR_FROM_RS["LSSTCam"].update(
    {
        f"{raft}_S{x}{y}": _LSSTCAM_CORNER_RAFTS[raft] + (0 if x == "G" else 2) + y
        for raft in _LSSTCAM_CORNER_RAFTS
        for x in ("G", "W")
        for y in range(2)
    }
)

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
    else:
        instrument = _TRANSLATE_INSTRUMENT.get(instrument, instrument)
        m = re.match(LSST_REGEXP, oid)
        if m:
            detector = _DETECTOR_FROM_RS[instrument][m["raft_sensor"]]
            return instrument == visit.instrument and detector == visit.detector

    return False


def get_prefix_from_snap(
    instrument: str, group: str, detector: int, snap: int
) -> str | None:
    """Compute path prefix for a raw image object from a data id.

    Parameters
    ----------
    instrument: `str`
        The name of the instrument taking the image.
    group: `str`
        The group id from the visit, associating the snaps making up the visit.
    detector: `int`
        The integer detector id for the image being sought.
    snap: `int`
        The snap number within the group for the visit.

    Returns
    -------
    prefix: `str` or None
        The prefix to a path to the corresponding raw image object.  If it
        can be calculated, then the prefix may be the entire path.  If no
        prefix can be calculated, None is returned.
    """

    if instrument not in _LSST_CAMERA_LIST:
        return f"{instrument}/{detector}/{group}/{snap}/"
    # TODO DM-39022: use a microservice to determine paths for LSST cameras.
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

    else:
        instrument = _TRANSLATE_INSTRUMENT.get(instrument, instrument)
        m = re.match(LSST_REGEXP, oid)
        if m:
            # Ignore instrument abbreviation and controller
            _, _, day_obs, seq_num = m["obs_id"].split("_")
            return int(day_obs) * 100000 + int(seq_num)

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

    m = re.match(LSST_REGEXP, oid)
    if not m:
        raise ValueError(f"{oid} could not be parsed into a group")
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

    day_obs = exposure_id // 100000
    seq_num = exposure_id % 100000
    abbrev = _CAMERA_ABBREV[instrument]
    raft_sensor = _DETECTOR_FROM_INT[instrument][detector]
    obs_id = f"{abbrev}_O_{day_obs}_{seq_num:06d}"
    return f"{instrument}/{day_obs}/{obs_id}/{obs_id}_{raft_sensor}.fits"
