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

__all__ = ["get_output_chain", "get_preload_run", "get_output_run", "get_day_obs"]


import logging
import os

import astropy.time

import lsst.obs.base


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)


def get_output_chain(instrument: lsst.obs.base.Instrument, date: str) -> str:
    """Generate a deterministic output chain name that avoids
    configuration conflicts.

    Parameters
    ----------
    instrument : `lsst.obs.base.Instrument`
        The instrument for which to generate a collection.
    date : `str`
        Date of the processing run (not observation!).

    Returns
    -------
    chain : `str`
        The chain in which to place all output collections.
    """
    # Order optimized for S3 bucket -- filter out as many files as soon as possible.
    return instrument.makeCollectionName("prompt", f"output-{date}")


def get_preload_run(instrument: lsst.obs.base.Instrument, deployment_id: str, date: str) -> str:
    """Generate a deterministic preload collection name that avoids
    configuration conflicts.

    Parameters
    ----------
    instrument : `lsst.obs.base.Instrument`
        The instrument for which to generate a collection.
    deployment_id : `str`
        A unique version ID of the active stack and pipeline configuration(s).
    date : `str`
        Date of the processing run (not observation!).

    Returns
    -------
    run : `str`
        The run in which to place preload/pre-execution products.
    """
    return get_output_run(instrument, deployment_id, "NoPipeline", date)


def get_output_run(instrument: lsst.obs.base.Instrument,
                   deployment_id: str,
                   pipeline_file: str,
                   date: str,
                   ) -> str:
    """Generate a deterministic collection name that avoids version or
    provenance conflicts.

    Parameters
    ----------
    instrument : `lsst.obs.base.Instrument`
        The instrument for which to generate a collection.
    deployment_id : `str`
        A unique version ID of the active stack and pipeline configuration(s).
    pipeline_file : `str`
        The pipeline name that the run will be used for.
    date : `str`
        Date of the processing run (not observation!).

    Returns
    -------
    run : `str`
        The run in which to place processing outputs.
    """
    pipeline_name, _ = os.path.splitext(os.path.basename(pipeline_file))
    # Order optimized for S3 bucket -- filter out as many files as soon as possible.
    return "/".join([get_output_chain(instrument, date), pipeline_name, deployment_id])


def get_day_obs(time: astropy.time.Time) -> str:
    """Convert a timestamp into a day-obs string.

    The day-obs is defined as the TAI date of an instant 12 hours before
    the timestamp.

    Parameters
    ----------
    time : `astropy.time.Time`
        The timestamp to convert.

    Returns
    -------
    day_obs : `str`
        The day-obs corresponding to ``time``, in YYYY-MM-DD format.
    """
    day_obs_delta = astropy.time.TimeDelta(-12.0 * astropy.units.hour, scale="tai")
    return (time + day_obs_delta).tai.to_value("iso", "date")
