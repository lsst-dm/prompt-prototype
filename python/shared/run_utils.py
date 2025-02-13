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

__all__ = ["get_output_chain", "get_preload_run", "get_output_run", "get_day_obs", "get_deployment"]


import glob
import hashlib
import logging
import os

import astropy.time

import lsst.obs.base


_log = logging.getLogger("lsst." + __name__)
_log.setLevel(logging.DEBUG)
_log_trace3 = logging.getLogger("TRACE3.lsst." + __name__)
_log_trace3.setLevel(logging.CRITICAL)  # Turn off by default.


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


def _get_pp_hash():
    """Get a unique ID for the Prompt Processing code.

    Returns
    ----------
    hash : `hashlib.hash`
        A hash of the contents of Prompt Processing.
    """
    root_dir = lsst.utils.getPackageDir("prompt_processing")
    files = []
    files.extend(sorted(glob.glob(os.path.join(root_dir, "maps", "**", "*.fits"), recursive=True)))
    files.extend(sorted(glob.glob(os.path.join(root_dir, "pipelines", "**", "*.yaml"), recursive=True)))
    # Source code has different paths in test environment and Docker container.
    # Use the same order for both, or the hash will be environment-dependent!
    files.extend(sorted(glob.glob(os.path.join(root_dir, "python", "activator", "*.py"), recursive=True)))
    files.extend(sorted(glob.glob(os.path.join(root_dir, "python", "initializer", "*.py"), recursive=True)))
    files.extend(sorted(glob.glob(os.path.join(root_dir, "python", "shared", "*.py"), recursive=True)))
    files.extend(sorted(glob.glob(os.path.join(root_dir, "activator", "*.py"), recursive=True)))
    files.extend(sorted(glob.glob(os.path.join(root_dir, "initializer", "*.py"), recursive=True)))
    files.extend(sorted(glob.glob(os.path.join(root_dir, "shared", "*.py"), recursive=True)))

    filehash = hashlib.md5(usedforsecurity=False)
    for file in files:
        with open(file, "rb") as f:
            filehash.update(f.read())  # Don't need to worry about OOM.
    return filehash


def get_deployment(apdb_config: str):
    """Get a unique version ID of the active stack and pipeline
    configuration(s).

    Parameters
    ----------
    apdb_config : `str`
        The location of the APDB config file (which is incorporated into the
        live pipeline config).

    Returns
    -------
    version : `str`
        A unique version identifier for the stack. Contains only
        word characters and "-", but not guaranteed to be human-readable.
    """
    # To disambiguate all stack changes, read the active Science Pipelines install.
    # getAllPythonDistributions misses non-Python Conda packages, but this should
    # be fine since rubin-env updates many Conda packages at once.
    packages = lsst.utils.packages.getAllPythonDistributions()
    packagehash = hashlib.md5(usedforsecurity=False)
    for package, version in sorted(packages.items()):
        _log_trace3.debug("Deployment includes %s %s.", package, version)
        packagehash.update(bytes(package + version, encoding="utf-8"))
    packagehash.update(_get_pp_hash().digest())

    # APDB config is included in pipeline/task configs.
    # Add other config fields as needed.
    confighash = hashlib.md5(usedforsecurity=False)
    confighash.update(bytes(apdb_config, encoding="utf-8"))

    version = f"pipelines-{packagehash.hexdigest():.7}-config-{confighash.hexdigest():.7}"
    _log.debug("Deployment identified as %s.", version)
    return version
