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

__all__ = ["get_output_chain", "get_preload_run", "get_output_run"]


import logging
import os

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
