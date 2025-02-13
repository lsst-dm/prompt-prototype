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


__all__ = ["main", "make_parser"]


import argparse
import logging
import os
import yaml

import astropy.time

import lsst.daf.butler
import lsst.pipe.base

from shared.config import PipelinesConfig
from shared.logger import setup_usdf_logger
from shared.run_utils import get_output_run, get_day_obs, get_deployment


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


def make_parser():
    parser = argparse.ArgumentParser()
    # Instrument, repo, pipelines configs, and apdb submitted through envvars
    # to parallel the activator. Pipelines are too complex for CLI anyway.
    parser.add_argument(
        "--deploy-id",
        required=False,
        help="Override the unique ID for this deployment. By default, an "
             "internal calculation is used.",
    )
    return parser


def main(args=None):
    """Generate init-outputs for a particular Prompt Processing configuration.

    Parameters
    ----------
    args : `list` [`str`]
        The command-line arguments for this program. Defaults to `sys.argv`.
    """
    instrument_name = os.environ["RUBIN_INSTRUMENT"]
    setup_usdf_logger(labels={"instrument": instrument_name},)

    parsed = make_parser().parse_args(args)
    # URI to the repository that should contain init-outputs.
    butler = lsst.daf.butler.Butler(os.environ["CALIB_REPO"], writeable=True)
    # The preprocessing pipelines to execute and the conditions in which to choose them.
    pre_pipelines = _config_from_yaml(os.environ["PREPROCESSING_PIPELINES_CONFIG"])
    # The main pipelines to execute and the conditions in which to choose them.
    main_pipelines = _config_from_yaml(os.environ["MAIN_PIPELINES_CONFIG"])
    # URI to an APDB config file.
    apdb = os.environ["CONFIG_APDB"]
    deploy_id = parsed.deploy_id or get_deployment(apdb)

    instrument = lsst.obs.base.Instrument.from_string(instrument_name, butler.registry)

    for pipeline in pre_pipelines.get_all_pipeline_files():
        _make_init_outputs(butler, instrument, apdb, deploy_id, pipeline)
    for pipeline in main_pipelines.get_all_pipeline_files():
        _make_init_outputs(butler, instrument, apdb, deploy_id, pipeline)


def _make_init_outputs(base_butler: lsst.daf.butler.Butler,
                       instrument: lsst.obs.base.Instrument,
                       apdb: str,
                       deployment_id: str,
                       pipe_file: str,
                       ):
    """Generate the init-outputs for a particular Prompt Processing pipeline.

    The init-outputs are automatically placed in the pipeline's correct
    output run.

    Parameters
    ----------
    base_butler : `lsst.daf.butler.Butler`
        A writeable Butler pointing to the output repository.
    instrument : `lsst.obs.base.Instrument`
        The instrument for which to generate init-outputs.
    apdb : `str`
        A URI to an APDB config file.
    deployment_id : `str`
        A unique identifier to distinguish runs with potentially different
        configurations.
    pipe_file : `str`
        An absolute path to the pipeline to process.

    Raises
    ------
    lsst.daf.butler.MissingDatasetTypeError
        Raised if the pipeline requires a dataset type that has not
        been registered.
    lsst.daf.butler.ConflictingDefinitionError
        Raised if the pipeline's definition of a dataset type does not match
        the existing one.
    """
    run = get_output_run(instrument, deployment_id, pipe_file, get_day_obs(astropy.time.Time.now()))
    output_butler = base_butler.clone(run=run)

    pipeline = lsst.pipe.base.Pipeline.fromFile(pipe_file)
    pipeline.addConfigOverride("parameters", "apdb_config", apdb)
    graph = pipeline.to_graph(output_butler.registry)
    # Don't register dataset types; automated registration in shared repos is frowned upon severely.
    graph.check_dataset_type_registrations(output_butler)

    graph.init_output_run(output_butler)
