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

import os.path
import tempfile
import unittest
import unittest.mock

import astropy.time

import lsst.dax.apdb
from lsst.daf.butler import Butler
import lsst.obs.base
import lsst.obs.lsst

from initializer.write_init_outputs import main, _make_init_outputs
from shared.run_utils import get_output_run


class InitOutputsTest(unittest.TestCase):
    """Test the write_init_outputs script with faked data.

    This class creates a fresh test repository for writing to. This means test
    setup takes a long time, so test cases should be written judiciously.
    """
    def _create_copied_repo(self):
        """Create a fresh repository that's a copy of the test data.

        This method sets self.repo and arranges cleanup; cleanup would
        be awkward if this method returned a Butler instead.
        """
        # Copy test data to fresh Butler to allow write tests.
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        data_repo = os.path.join(data_dir, "central_repo")
        data_butler = Butler(data_repo, writeable=False)
        self.repo = tempfile.TemporaryDirectory()
        # TemporaryDirectory warns on leaks
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, self.repo)

        # Butler.transfer_from can't easily copy collections, so use
        # export/import instead.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as export_file:
            with data_butler.export(filename=export_file.name) as export:
                export.saveDatasets(data_butler.registry.queryDatasets(..., collections=...))
                for collection in data_butler.registry.queryCollections():
                    export.saveCollection(collection)
            dimension_config = data_butler.dimensions.dimensionConfig
            output_butler = Butler(Butler.makeRepo(self.repo.name, dimensionConfig=dimension_config),
                                   writeable=True,
                                   )
            output_butler.import_(directory=data_repo, filename=export_file.name, transfer="auto")

    def setUp(self):
        self._create_copied_repo()
        self.base_butler = lsst.daf.butler.Butler(self.repo.name, writeable=True)
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        self.input_data = os.path.join(data_dir, "input_data")

        config = lsst.dax.apdb.ApdbSql.init_database(db_url=f"sqlite:///{self.repo.name}/apdb.db")
        self.config_file = tempfile.NamedTemporaryFile(suffix=".py")
        self.addCleanup(self.config_file.close)
        config.save(self.config_file.name)

        self.deploy_id = "pipelines-1234567-config-abcdef0"

    def _register_dataset_types(self, pipeline_file):
        graph = lsst.pipe.base.Pipeline.fromFile(pipeline_file).to_graph(self.base_butler.registry)
        graph.register_dataset_types(self.base_butler)

    def test_not_registered(self):
        pipe_file = "${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml"
        instrument = lsst.obs.base.Instrument.from_string("lsst.obs.lsst.LsstComCamSim")

        with self.assertRaises(lsst.daf.butler.MissingDatasetTypeError):
            _make_init_outputs(self.base_butler, instrument, self.config_file.name, self.deploy_id, pipe_file)

    def test_empty_run(self):
        pipe_file = "${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml"
        instrument = lsst.obs.base.Instrument.from_string("lsst.obs.lsst.LsstComCamSim")
        expected_run = get_output_run(instrument, self.deploy_id, pipe_file, "2024-09-24")

        self._register_dataset_types(pipe_file)

        with unittest.mock.patch("astropy.time.Time.now",
                                 return_value=astropy.time.Time("2024-09-24T15:00")):
            _make_init_outputs(self.base_butler, instrument, self.config_file.name, self.deploy_id, pipe_file)

        self.base_butler.registry.refresh()
        self.assertTrue(self.base_butler.collections.query(expected_run))
        config_types = set(self.base_butler.registry.queryDatasetTypes("*_config"))
        self.assertEqual({t.name for t in config_types}, {"isr_config", "calibrateImage_config"})
        for t in config_types:
            configs = self.base_butler.query_datasets(t, expected_run)
            self.assertEqual(len(configs), 1)

    def test_filled_run(self):
        pipe_file = "${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml"
        instrument = lsst.obs.base.Instrument.from_string("lsst.obs.lsst.LsstComCamSim")
        expected_run = get_output_run(instrument, self.deploy_id, pipe_file, "2024-09-24")

        self._register_dataset_types(pipe_file)

        with unittest.mock.patch("astropy.time.Time.now",
                                 return_value=astropy.time.Time("2024-09-24T15:00")):
            _make_init_outputs(self.base_butler, instrument, self.config_file.name, self.deploy_id, pipe_file)
            # Should be a no-op
            _make_init_outputs(self.base_butler, instrument, self.config_file.name, self.deploy_id, pipe_file)

        self.base_butler.registry.refresh()
        self.assertTrue(self.base_butler.collections.query(expected_run))
        config_types = set(self.base_butler.registry.queryDatasetTypes("*_config"))
        self.assertEqual({t.name for t in config_types}, {"isr_config", "calibrateImage_config"})
        for t in config_types:
            configs = self.base_butler.query_datasets(t, expected_run)
            self.assertEqual(len(configs), 1)

    def test_main(self):
        # Main does its own parsing, so need the stringified configs.
        pipelines = \
            '''- survey: SURVEY
  pipelines:
  - ${PROMPT_PROCESSING_DIR}/tests/data/ApPipe.yaml
  - ${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml
            '''
        pre_pipelines = \
            '''- survey: SURVEY
  pipelines:
  - ${PROMPT_PROCESSING_DIR}/tests/data/Preprocess.yaml
  - ${PROMPT_PROCESSING_DIR}/tests/data/MinPrep.yaml
            '''

        with unittest.mock.patch.dict(os.environ, {"RUBIN_INSTRUMENT": "LSSTComCamSim",
                                                   "PREPROCESSING_PIPELINES_CONFIG": pre_pipelines,
                                                   "MAIN_PIPELINES_CONFIG": pipelines,
                                                   "CALIB_REPO": self.repo.name,
                                                   "CONFIG_APDB": self.config_file.name,
                                                   }), \
                unittest.mock.patch("initializer.write_init_outputs._make_init_outputs") as mock_make:
            main(["--deploy-id", self.deploy_id])

        calls = mock_make.call_args_list
        self.assertEqual(len(calls), 4)
        # Execution order is undefined
        for pipeline in ["${PROMPT_PROCESSING_DIR}/tests/data/ApPipe.yaml",
                         "${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml",
                         "${PROMPT_PROCESSING_DIR}/tests/data/Preprocess.yaml",
                         "${PROMPT_PROCESSING_DIR}/tests/data/MinPrep.yaml",
                         ]:
            full_pipeline = os.path.expandvars(pipeline)
            self.assertIn(
                unittest.mock.call(unittest.mock.ANY,  # Butler doesn't support useful comparisons
                                   unittest.mock.ANY,  # Instrument doesn't support equality comparisons
                                   self.config_file.name,
                                   self.deploy_id,
                                   full_pipeline),
                calls)
        for call in calls:
            arg_instrument = call.args[1]
            self.assertIsInstance(arg_instrument, lsst.obs.lsst.LsstComCamSim)
