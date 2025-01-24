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
import datetime
import functools
import itertools
import tempfile
import os.path
import shutil
import unittest
import unittest.mock
import warnings

import astropy.coordinates
import astropy.table
import astropy.time
import astropy.units as u
import erfa
import psycopg2

import astro_metadata_translator
import lsst.pex.config
import lsst.afw.image
import lsst.afw.table
from lsst.dax.apdb import ApdbSql
from lsst.daf.butler import Butler, CollectionType, DataCoordinate, DimensionUniverse
import lsst.daf.butler.tests as butler_tests
from lsst.obs.base.formatters.fitsExposure import FitsImageFormatter
from lsst.obs.base.ingest import RawFileDatasetInfo, RawFileData
from lsst.pipe.base import QuantumGraph
from lsst.pipe.base.tests.simpleQGraph import makeSimpleQGraph
import lsst.resources
import lsst.sphgeom

from activator.caching import DatasetCache
from activator.config import PipelinesConfig
from activator.exception import NonRetriableError, NoGoodPipelinesError, PipelineExecutionError
from activator.visit import FannedOutVisit
from activator.middleware_interface import get_central_butler, flush_local_repo, make_local_repo, \
    _get_sasquatch_dispatcher, MiddlewareInterface, \
    _filter_datasets, _generic_query, _filter_calibs_by_date, _MissingDatasetError

# The short name of the instrument used in the test repo.
instname = "LSSTComCamSim"
# Full name of the physical filter for the test file.
filter = "g_01"
# The skymap name used in the test repo.
skymap_name = "ops_rehersal_prep_2k_v1"
# A pipelines config that returns the test pipelines.
# Unless a test imposes otherwise, the first pipeline should run, and
# the second should not be attempted.
pipelines = PipelinesConfig([{"survey": "SURVEY",
                              "pipelines": ["${PROMPT_PROCESSING_DIR}/tests/data/ApPipe.yaml",
                                            "${PROMPT_PROCESSING_DIR}/tests/data/SingleFrame.yaml",
                                            ],
                              }])
pipelines_minimal = PipelinesConfig([{"survey": "SURVEY",
                                      "pipelines": ["${PROMPT_PROCESSING_DIR}/tests/data/ISR.yaml", ],
                                      }])
pre_pipelines_empty = PipelinesConfig([{"survey": "SURVEY", "pipelines": None}])
pre_pipelines_full = PipelinesConfig([{"survey": "SURVEY",
                                       "pipelines": ["${PROMPT_PROCESSING_DIR}/tests/data/Preprocess.yaml",
                                                     "${PROMPT_PROCESSING_DIR}/tests/data/MinPrep.yaml",
                                                     ],
                                       }])


def fake_file_data(filename, dimensions, instrument, visit):
    """Return file data for a mock file to be ingested.

    Parameters
    ----------
    filename : `str`
        Full path to the file to mock. Can be a non-existant file.
    dimensions : `lsst.daf.butler.DimensionsUniverse`
        The full set of dimensions for this butler.
    instrument : `lsst.obs.base.Instrument`
        The instrument the file is supposed to be from.
    visit : `FannedOutVisit`
        Group of snaps from one detector to be processed.

    Returns
    -------
    data_id, file_data, : `DataCoordinate`, `RawFileData`
        The id and descriptor for the mock file.
    """
    exposure_id = int(visit.groupId)
    data_id = DataCoordinate.standardize({"exposure": exposure_id,
                                          "detector": visit.detector,
                                          "instrument": instrument.getName()},
                                         universe=dimensions)

    start_time = astropy.time.Time("2024-06-17T22:06:15", scale="tai")
    day_obs = 20240617
    obs_info = astro_metadata_translator.makeObservationInfo(
        instrument=instrument.getName(),
        datetime_begin=start_time,
        datetime_end=start_time + 30*u.second,
        exposure_id=exposure_id,
        exposure_group=visit.groupId,
        visit_id=exposure_id,
        boresight_rotation_angle=astropy.coordinates.Angle(visit.cameraAngle*u.degree),
        boresight_rotation_coord=visit.rotationSystem.name.lower(),
        tracking_radec=astropy.coordinates.SkyCoord(*visit.position, frame="icrs", unit="deg"),
        observation_id=visit.groupId,
        physical_filter=filter,
        exposure_time=30.0*u.second,
        observation_type="science",
        observing_day=day_obs,
        group_counter_start=exposure_id,
        group_counter_end=exposure_id,
    )
    dataset_info = RawFileDatasetInfo(data_id, obs_info)
    file_data = RawFileData([dataset_info],
                            lsst.resources.ResourcePath(filename),
                            FitsImageFormatter,
                            instrument)
    return data_id, file_data


# TODO: merge this into fake_file_data after DM-46152
def fake_eng_data(filename, dimensions, instrument, visit):
    """Return file data for a mock non-science file to be ingested.

    Parameters
    ----------
    filename : `str`
        Full path to the file to mock. Can be a non-existant file.
    dimensions : `lsst.daf.butler.DimensionsUniverse`
        The full set of dimensions for this butler.
    instrument : `lsst.obs.base.Instrument`
        The instrument the file is supposed to be from.
    visit : `FannedOutVisit`
        Group of snaps from one detector to be processed.

    Returns
    -------
    data_id, file_data, : `DataCoordinate`, `RawFileData`
        The id and descriptor for the mock file.
    """
    exposure_id = int(visit.groupId)
    data_id = DataCoordinate.standardize({"exposure": exposure_id,
                                          "detector": visit.detector,
                                          "instrument": instrument.getName()},
                                         universe=dimensions)

    start_time = astropy.time.Time("2024-06-17T22:06:15", scale="tai")
    day_obs = 20240617
    obs_info = astro_metadata_translator.makeObservationInfo(
        instrument=instrument.getName(),
        datetime_begin=start_time,
        datetime_end=start_time + 30*u.second,
        exposure_id=exposure_id,
        exposure_group=visit.groupId,
        visit_id=None,
        boresight_rotation_angle=None,
        boresight_rotation_coord=None,
        tracking_radec=None,
        observation_id=visit.groupId,
        physical_filter=filter,
        exposure_time=30.0*u.second,
        observation_type="goofing off",
        observing_day=day_obs,
        group_counter_start=exposure_id,
        group_counter_end=exposure_id,
    )
    dataset_info = RawFileDatasetInfo(data_id, obs_info)
    file_data = RawFileData([dataset_info],
                            lsst.resources.ResourcePath(filename),
                            FitsImageFormatter,
                            instrument)
    return data_id, file_data


class MiddlewareInterfaceTest(unittest.TestCase):
    """Test the MiddlewareInterface class with faked data.
    """
    def setUp(self):
        self.data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        self.central_repo = os.path.join(self.data_dir, "central_repo")
        self.umbrella = f"{instname}/defaults"
        self.central_butler = Butler(self.central_repo,
                                     collections=[self.umbrella],
                                     writeable=False,
                                     inferDefaults=False)
        self.input_data = os.path.join(self.data_dir, "input_data")
        self.local_repo = make_local_repo(tempfile.gettempdir(), self.central_butler, instname)
        self.local_cache = DatasetCache(3, {"uw_stars_20240524": 10, "goodSeeingCoadd": 30})
        self.addCleanup(self.local_repo.cleanup)  # TemporaryDirectory warns on leaks

        config = ApdbSql.init_database(db_url=f"sqlite:///{self.local_repo.name}/apdb.db")
        config_file = tempfile.NamedTemporaryFile(suffix=".py")
        self.addCleanup(config_file.close)
        config.save(config_file.name)

        env_patcher = unittest.mock.patch.dict(os.environ,
                                               {"CONFIG_APDB": config_file.name,
                                                })
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

        # coordinates from OR4 visit 7024061700046
        ra = 215.82729413263485
        dec = -12.4705546590231
        rot = 149.86873311284756
        self.next_visit = FannedOutVisit(instrument=instname,
                                         detector=4,
                                         groupId="1",
                                         nimages=1,
                                         filters=filter,
                                         coordinateSystem=FannedOutVisit.CoordSys.ICRS,
                                         position=[ra, dec],
                                         startTime=1718661950.0,
                                         rotationSystem=FannedOutVisit.RotSys.SKY,
                                         cameraAngle=rot,
                                         survey="SURVEY",
                                         salIndex=3,
                                         scriptSalIndex=3,
                                         dome=FannedOutVisit.Dome.OPEN,
                                         duration=35.0,
                                         totalCheckpoints=1,
                                         private_sndStamp=1718661900.7165175,
                                         )
        self.logger_name = "lsst.activator.middleware_interface"
        self.interface = MiddlewareInterface(self.central_butler, self.input_data, self.next_visit,
                                             # TODO: replace pre_pipelines_empty on DM-43418
                                             pre_pipelines_empty, pipelines, skymap_name,
                                             self.local_repo.name, self.local_cache,
                                             prefix="file://")
        self.deploy_id = self.interface._deployment

    def test_get_butler(self):
        for butler in [get_central_butler(self.central_repo, "lsst.obs.lsst.LsstComCamSim"),
                       get_central_butler(self.central_repo, instname),
                       ]:
            # TODO: better way to test repo location?
            self.assertTrue(
                butler.getURI("skyMap", skymap=skymap_name, run="foo", predict=True).ospath
                .startswith(self.central_repo))
            self.assertEqual(list(butler.collections.defaults), [f"{instname}/defaults"])
            self.assertTrue(butler.isWriteable())

    def test_make_local_repo(self):
        for inst in [instname, "lsst.obs.lsst.LsstComCamSim"]:
            with make_local_repo(tempfile.gettempdir(), Butler(self.central_repo), inst) as repo_dir:
                self.assertTrue(os.path.exists(repo_dir))
                butler = Butler(repo_dir)
                self.assertEqual([x.dataId for x in butler.registry.queryDimensionRecords("instrument")],
                                 [DataCoordinate.standardize({"instrument": instname},
                                                             universe=butler.dimensions)])
                self.assertIn(f"{instname}/defaults", butler.registry.queryCollections())
            self.assertFalse(os.path.exists(repo_dir))

    def test_init(self):
        """Basic tests of the initialized interface object.
        """
        # Ideas for things to test:
        # * On init, does the right kind of butler get created, with the right
        #   collections, etc?
        # * On init, is the local butler repo purely in memory?

        # Check that the butler instance is properly configured.
        instruments = list(self.interface.butler.registry.queryDimensionRecords("instrument"))
        self.assertEqual(instname, instruments[0].name)
        self.assertEqual(set(self.interface.butler.collections.defaults), {self.umbrella})

        # Check that the ingester is properly configured.
        self.assertEqual(self.interface.rawIngestTask.config.failFast, True)
        self.assertEqual(self.interface.rawIngestTask.config.transfer, "copy")

    def _check_imports(self, butler, group, detector, expected_shards, have_filter=True):
        """Test that the butler has the expected supporting data.
        """
        self.assertEqual(butler.get('camera',
                                    instrument=instname,
                                    collections=[f"{instname}/calib/unbounded"]).getName(), instname)

        # Check that the right skymap is in the chained output collection.
        self.assertTrue(
            butler.exists("skyMap",
                          skymap=skymap_name,
                          full_check=True,
                          collections=self.umbrella)
        )

        # check that we got appropriate refcat shards
        loaded_shards = butler.registry.queryDataIds("htm7",
                                                     datasets="uw_stars_20240524",
                                                     collections="refcats")

        self.assertEqual(expected_shards, {x['htm7'] for x in loaded_shards})
        # Check that the right calibs are in the chained output collection.
        self.assertTrue(
            butler.exists('bias', detector=detector, instrument='LSSTComCamSim',
                          full_check=True,
                          # TODO DM-46178: add query by validity range.
                          collections=self.umbrella)
        )
        if have_filter:
            self.assertTrue(
                butler.exists('flat', detector=detector, instrument='LSSTComCamSim',
                              physical_filter=filter,
                              full_check=True,
                              # TODO DM-46178: add query by validity range.
                              collections=self.umbrella)
            )
        # Check that we got a model (only one in the test data)
        self.assertTrue(
            butler.exists('pretrainedModelPackage',
                          full_check=True,
                          collections=self.umbrella)
        )

        if have_filter:
            # Check that the right templates are in the chained output collection.
            # Need to refresh the butler to get all the dimensions/collections.
            butler.registry.refresh()
            for patch in (142, 143, 158, 159, 160, 161, 175, 176, 177, 178,
                          192, 193, 194, 195, 210, 211,):
                with self.subTest(tract=7445, patch=patch):
                    self.assertTrue(
                        butler.exists('goodSeeingCoadd', tract=7445, patch=patch, band="g",
                                      skymap=skymap_name,
                                      full_check=True,
                                      collections=self.umbrella)
                    )
            with self.subTest(tract=7445, patch=0):
                self.assertFalse(
                    butler.exists('goodSeeingCoadd', tract=7445, patch=0, band="g",
                                  skymap=skymap_name,
                                  full_check=True,
                                  collections=self.umbrella)
                )

        # Check that preloaded datasets have been generated
        date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-12)))
        preload_collection = f"{instname}/prompt/output-{date.year:04d}-{date.month:02d}-{date.day:02d}/" \
                             f"Preload/{self.deploy_id}"
        self.assertTrue(
            butler.exists('promptPreload_metrics', instrument=instname, group=group, detector=detector,
                          full_check=True,
                          collections=preload_collection)
        )
        self.assertTrue(
            butler.exists('regionTimeInfo', instrument=instname, group=group, detector=detector,
                          full_check=True,
                          collections=preload_collection)
        )

    def test_prep_butler(self):
        """Test that the butler has all necessary data for the next visit.
        """
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                as mock_pre:
            self.interface.prep_butler()

        expected_shards = {166464, 177536}
        self._check_imports(self.interface.butler, group="1", detector=4,
                            expected_shards=expected_shards)

        # Hard to test actual pipeline output, so just check we're calling it
        mock_pre.assert_called_once()

    def test_prep_butler_olddate(self):
        """Test that prep_butler returns only calibs from a particular date range.
        """
        self.interface.visit = dataclasses.replace(
            self.interface.visit,
            private_sndStamp=datetime.datetime.fromisoformat("20150313T000000Z").timestamp(),
        )
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                as mock_pre:
            self.interface.prep_butler()

        expected_shards = {166464, 177536}
        with self.assertRaises((AssertionError, lsst.daf.butler.registry.MissingCollectionError)):
            # Nothing should have been imported
            self._check_imports(self.interface.butler, group="1", detector=4,
                                expected_shards=expected_shards)

        # Hard to test actual pipeline output, so just check we're calling it
        mock_pre.assert_called_once()

    def test_prep_butler_nofilter(self):
        """Test that prep_butler can handle visits without a filter.
        """
        self.interface.visit = dataclasses.replace(
            self.interface.visit,
            filters="",
        )
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                as mock_pre:
            self.interface.prep_butler()

        expected_shards = {166464, 177536}
        self._check_imports(self.interface.butler, group="1", detector=4,
                            expected_shards=expected_shards, have_filter=False)

        # Hard to test actual pipeline output, so just check we're calling it
        mock_pre.assert_called_once()

    def test_prep_butler_notemplates(self):
        """Test that prep_butler can handle pipeline configs without templates.
        """
        self.interface.main_pipelines = pipelines_minimal
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                as mock_pre, \
                self.assertNoLogs(level="ERROR"):
            self.interface.prep_butler()

        # Hard to test actual pipeline output, so just check we're calling it
        mock_pre.assert_called_once()

    # TODO: prep_butler doesn't know what kinds of calibs to expect, so can't
    # tell that there are specifically, e.g., no flats. This test should pass
    # as-is after DM-40245.
    @unittest.expectedFailure
    def test_prep_butler_novalid(self):
        """Test that prep_butler raises if no calibs are currently valid.
        """
        self.interface.visit = dataclasses.replace(
            self.interface.visit,
            private_sndStamp=datetime.datetime(2050, 1, 1).timestamp(),
        )

        with warnings.catch_warnings():
            # Avoid "dubious year" warnings from using a 2050 date
            warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            with self.assertRaises(_MissingDatasetError), \
                unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                    as mock_pre:
                self.interface.prep_butler()

        mock_pre.assert_not_called()

    def test_prep_butler_twice(self):
        """prep_butler should have the correct calibs (and not raise an
        exception!) on a second run with the same, or a different detector.
        This explicitly tests the "you can't import something that's already
        in the local butler" problem that's related to the "can't register
        the skymap in init" problem.
        """
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                as mock_pre:
            self.interface.prep_butler()

        # Second visit with everything same except group.
        second_visit = dataclasses.replace(self.next_visit, groupId=str(int(self.next_visit.groupId) + 1))
        second_interface = MiddlewareInterface(self.central_butler, self.input_data, second_visit,
                                               # TODO: replace pre_pipelines_empty on DM-43418
                                               pre_pipelines_empty, pipelines, skymap_name,
                                               self.local_repo.name, self.local_cache,
                                               prefix="file://")

        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                as mock_pre:
            second_interface.prep_butler()
        expected_shards = {166464, 177536}
        self._check_imports(second_interface.butler, group="2", detector=4,
                            expected_shards=expected_shards)
        # Hard to test actual pipeline output, so just check we're calling it
        mock_pre.assert_called_once()

        # Third visit with different detector and coordinates.
        # Only 4 and 5 have valid calibs.
        third_visit = dataclasses.replace(second_visit,
                                          detector=5,
                                          groupId=str(int(second_visit.groupId) + 1),
                                          # Offset to put detector=5 in same templates.
                                          position=[self.next_visit.position[0] - 0.2,
                                                    self.next_visit.position[1] - 0.1],
                                          )
        third_interface = MiddlewareInterface(self.central_butler, self.input_data, third_visit,
                                              # TODO: replace pre_pipelines_empty on DM-43418
                                              pre_pipelines_empty, pipelines, skymap_name,
                                              self.local_repo.name, self.local_cache,
                                              prefix="file://")
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing") \
                as mock_pre:
            third_interface.prep_butler()
        expected_shards.update({166464, 177536})
        self._check_imports(third_interface.butler, group="3", detector=5,
                            expected_shards=expected_shards)
        # Hard to test actual pipeline output, so just check we're calling it
        mock_pre.assert_called_once()

    def test_ingest_image(self):
        self.interface.prep_butler()  # Ensure raw collections exist.
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        data_id, file_data = fake_file_data(filepath,
                                            self.interface.butler.dimensions,
                                            self.interface.instrument,
                                            self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            exp_id = self.interface.ingest_image(filename)
            self.assertEqual(exp_id, int(self.next_visit.groupId))

            datasets = list(self.interface.butler.registry.queryDatasets('raw',
                                                                         collections=[f'{instname}/raw/all']))
            self.assertEqual(datasets[0].dataId, data_id)
            # TODO: After raw ingest, we can define exposure dimension records
            # and check that the visits are defined

    def test_ingest_image_fails_missing_file(self):
        """Trying to ingest a non-existent file should raise.

        NOTE: this is currently a bit of a placeholder: I suspect we'll want to
        change how errors are handled in the interface layer, raising custom
        exceptions so that the activator can deal with them better. So even
        though all this is demonstrating is that if the file doesn't exist,
        rawIngestTask.run raises FileNotFoundError and that gets passed up
        through ingest_image(), we'll want to have a test of "missing file
        ingestion", and this can serve as a starting point.
        """
        self.interface.prep_butler()  # Ensure raw collections exist.
        filename = "nonexistentImage.fits"
        filepath = os.path.join(self.input_data, filename)
        _, file_data = fake_file_data(filepath,
                                      self.interface.butler.dimensions,
                                      self.interface.instrument,
                                      self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock, \
                self.assertRaisesRegex(FileNotFoundError, "Resource at .* does not exist"):
            mock.return_value = file_data
            self.interface.ingest_image(filename)
        # There should not be any raw files in the registry.
        datasets = list(self.interface.butler.registry.queryDatasets('raw',
                                                                     collections=[f'{instname}/raw/all']))
        self.assertEqual(datasets, [])

    def _prepare_run_preprocessing(self):
        # Have to setup the data so that we can create the pipeline executor.
        self.interface.prep_butler()

    def _prepare_run_pipeline(self):
        # Have to setup the data so that we can create the pipeline executor.
        self._prepare_run_preprocessing()

        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        _, file_data = fake_file_data(filepath,
                                      self.interface.butler.dimensions,
                                      self.interface.instrument,
                                      self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            self.interface.ingest_image(filename)
        # Dummy "engineering" visit to test non-science handling
        eng_visit = dataclasses.replace(self.next_visit,
                                        groupId="42",
                                        coordinateSystem=FannedOutVisit.CoordSys.NONE,
                                        position=[0.0, 0.0],
                                        rotationSystem=FannedOutVisit.RotSys.NONE,
                                        cameraAngle=0.0,
                                        dome=FannedOutVisit.Dome.CLOSED,
                                        )
        _, eng_data = fake_eng_data(filepath,
                                    self.interface.butler.dimensions,
                                    self.interface.instrument,
                                    eng_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = eng_data
            self.interface.ingest_image(filename)
        # TODO: add any preprocessing outputs the main pipeline depends on (DM-43418?)

    def test_run_pipeline(self):
        """Test that running the pipeline uses the correct arguments.

        We can't run an actual pipeline because raw/calib/refcat/template data
        are all zeroed out.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph") \
                as mock_preexec, \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run:
            with self.assertLogs(self.logger_name, level="INFO") as logs:
                self.interface.run_pipeline({1})
        # Pre-execution and execution should only run once, even if graph
        # generation is attempted for multiple pipelines.
        mock_preexec.assert_called_once()
        # Pre-execution may have other arguments as needed; no requirement either way.
        self.assertEqual(mock_preexec.call_args.kwargs["register_dataset_types"], True)
        mock_run.assert_called_once()
        # Check that we configured the right pipeline.
        self.assertIn(os.path.join(self.data_dir, 'ApPipe.yaml'), "\n".join(logs.output))

    def _check_run_pipeline_fallback(self, callable, pipe_files, graphs, final_label):
        """Generic test for different fallback scenarios.

        Parameters
        ----------
        callable : callable [[]]
            A nullary callable that runs the target pipeline(s).
        pipe_files : sequence [`str`]
            The list of pipeline files configured for a visit.
        graphs : sequence [`collections.abc.Sized`]
            The list of quantum graphs (or suitable mocks) generated for each
            pipeline. Must have the same length as ``pipe_files``.
        final_label : `str`
            The description of the pipeline that should be run, given
            ``pipe_files`` and ``graphs``.
        """
        with unittest.mock.patch(
            "activator.middleware_interface.MiddlewareInterface._get_pre_pipeline_files",
            return_value=pipe_files), \
                unittest.mock.patch(
                    "activator.middleware_interface.MiddlewareInterface._get_main_pipeline_files",
                    return_value=pipe_files), \
                unittest.mock.patch(
                    "activator.middleware_interface.SeparablePipelineExecutor.make_quantum_graph",
                    side_effect=graphs), \
                unittest.mock.patch(
                    "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
                unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
                self.assertLogs(self.logger_name, level="INFO") as logs:
            callable()
        mock_run.assert_called_once()
        # Check that we configured the right pipeline.
        self.assertIn(final_label, "\n".join(logs.output))

    def test_run_pipeline_fallback_1failof2(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(2)]
        expected = "SingleFrame.yaml"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(lambda: self.interface.run_pipeline({1}),
                                          pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_1failof2_inverse(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml')]
        graph_list = [self._make_test_graph(2), self._make_test_graph(0)]
        expected = "ApPipe.yaml"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(lambda: self.interface.run_pipeline({1}),
                                          pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_2failof2(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(0)]
        expected = ""

        self._prepare_run_pipeline()
        with self.assertRaises(NoGoodPipelinesError):
            self._check_run_pipeline_fallback(lambda: self.interface.run_pipeline({1}),
                                              pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_0failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [self._make_test_graph(2), self._make_test_graph(2), self._make_test_graph(0)]
        expected = "ApPipe.yaml"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(lambda: self.interface.run_pipeline({1}),
                                          pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_1failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(2), self._make_test_graph(1)]
        expected = "SingleFrame.yaml"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(lambda: self.interface.run_pipeline({1}),
                                          pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_2failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(0), self._make_test_graph(1)]
        expected = "ISR.yaml"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(lambda: self.interface.run_pipeline({1}),
                                          pipe_list, graph_list, expected)

    def test_run_pipeline_fallback_2failof3_inverse(self):
        pipe_list = [os.path.join(self.data_dir, 'ApPipe.yaml'),
                     os.path.join(self.data_dir, 'SingleFrame.yaml'),
                     os.path.join(self.data_dir, 'ISR.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(2), self._make_test_graph(0)]
        expected = "SingleFrame.yaml"

        self._prepare_run_pipeline()
        self._check_run_pipeline_fallback(lambda: self.interface.run_pipeline({1}),
                                          pipe_list, graph_list, expected)

    def test_run_pipeline_bad_visits(self):
        """Test that running a pipeline that results in bad visit definition
        (because the exposure ids are wrong), raises.
        """
        # Have to setup the data so that we can create the pipeline executor.
        self.interface.prep_butler()
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        _, file_data = fake_file_data(filepath,
                                      self.interface.butler.dimensions,
                                      self.interface.instrument,
                                      self.next_visit)
        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            self.interface.ingest_image(filename)

        with self.assertRaisesRegex(RuntimeError, "No data to process"):
            self.interface.run_pipeline({2})

    def test_run_pipeline_early_exception(self):
        """Test behavior when execution fails in single-frame processing.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch("lsst.dax.apdb.ApdbSql.containsVisitDetector") as mock_query:
            mock_run.side_effect = ValueError("Error: not computable")
            mock_query.return_value = False
            with self.assertRaises(PipelineExecutionError):
                self.interface.run_pipeline({1})

    def test_run_pipeline_late_exception(self):
        """Test behavior when execution fails in diaPipe cleanup.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch("lsst.dax.apdb.ApdbSql.containsVisitDetector") as mock_query:
            mock_run.side_effect = ValueError("Error: not computable")
            mock_query.return_value = True
            with self.assertRaises(NonRetriableError):
                self.interface.run_pipeline({1})

    def test_run_pipeline_cascading_exception(self):
        """Test behavior when Butler and/or APDB access has failed completely.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch("lsst.dax.apdb.ApdbSql.containsVisitDetector") as mock_query:
            mock_run.side_effect = ValueError("Error: not computable")
            mock_query.side_effect = psycopg2.OperationalError("Database? What database?")
            with self.assertRaises(NonRetriableError):
                self.interface.run_pipeline({1})

    def test_run_pipeline_early_exception_novisit(self):
        """Test behavior when execution fails in ISR-only processing.
        """
        self._prepare_run_pipeline()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph"), \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch.object(self.interface, "main_pipelines", pipelines_minimal), \
             unittest.mock.patch("lsst.dax.apdb.ApdbSql.containsVisitDetector") as mock_query:
            mock_run.side_effect = ValueError("Error: not computable")
            mock_query.return_value = False
            with self.assertRaises(PipelineExecutionError):
                # Engineering run; see _prepare_run_pipeline
                self.interface.run_pipeline({42})

    def test_run_preprocessing_empty(self):
        """Test that running the preprocessiing pipeline does nothing if no
        pipelines configured.
        """
        self._prepare_run_preprocessing()

        with self.assertLogs(self.logger_name, level="INFO") as logs:
            self.interface._run_preprocessing()
        self.assertIn("skipping", "\n".join(logs.output))
        # Check that no pipelines mentioned
        self.assertNotIn(os.path.join(self.data_dir, 'Preprocess.yaml'), "\n".join(logs.output))

    def test_run_preprocessing_full(self):
        """Test that running the preprocessiing pipeline uses the correct arguments.

        We can't run an actual pipeline because all data are zeroed out.
        """
        self._prepare_run_preprocessing()

        with unittest.mock.patch(
                "activator.middleware_interface.SeparablePipelineExecutor.pre_execute_qgraph") \
                as mock_preexec, \
             unittest.mock.patch("activator.middleware_interface.SeparablePipelineExecutor.run_pipeline") \
                as mock_run, \
             unittest.mock.patch.object(self.interface, "pre_pipelines", pre_pipelines_full):
            with self.assertLogs(self.logger_name, level="INFO") as logs:
                self.interface._run_preprocessing()
        # Pre-execution and execution should only run once, even if graph
        # generation is attempted for multiple pipelines.
        mock_preexec.assert_called_once()
        # Pre-execution may have other arguments as needed; no requirement either way.
        self.assertEqual(mock_preexec.call_args.kwargs["register_dataset_types"], True)
        mock_run.assert_called_once()
        # Check that we configured the right pipeline.
        self.assertIn(os.path.join(self.data_dir, 'Preprocess.yaml'), "\n".join(logs.output))

    def test_run_preprocessing_fallback_1failof2(self):
        pipe_list = [os.path.join(self.data_dir, 'Preprocess.yaml'),
                     os.path.join(self.data_dir, 'MinPrep.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(2)]
        expected = "MinPrep.yaml"

        self._prepare_run_preprocessing()
        self._check_run_pipeline_fallback(self.interface._run_preprocessing, pipe_list, graph_list, expected)

    def test_run_preprocessing_fallback_1failof2_inverse(self):
        pipe_list = [os.path.join(self.data_dir, 'Preprocess.yaml'),
                     os.path.join(self.data_dir, 'MinPrep.yaml')]
        graph_list = [self._make_test_graph(2), self._make_test_graph(0)]
        expected = "Preprocess.yaml"

        self._prepare_run_preprocessing()
        self._check_run_pipeline_fallback(self.interface._run_preprocessing, pipe_list, graph_list, expected)

    def test_run_preprocessing_fallback_2failof2(self):
        pipe_list = [os.path.join(self.data_dir, 'Preprocess.yaml'),
                     os.path.join(self.data_dir, 'MinPrep.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(0)]
        expected = ""

        self._prepare_run_preprocessing()
        with self.assertRaises(NoGoodPipelinesError):
            self._check_run_pipeline_fallback(self.interface._run_preprocessing,
                                              pipe_list, graph_list, expected)

    def test_run_preprocessing_fallback_0failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'Preprocess.yaml'),
                     os.path.join(self.data_dir, 'MinPrep.yaml'),
                     os.path.join(self.data_dir, 'NoPrep.yaml')]
        graph_list = [self._make_test_graph(2), self._make_test_graph(2), self._make_test_graph(1)]
        expected = "Preprocess.yaml"

        self._prepare_run_preprocessing()
        self._check_run_pipeline_fallback(self.interface._run_preprocessing, pipe_list, graph_list, expected)

    def test_run_preprocessing_fallback_1failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'Preprocess.yaml'),
                     os.path.join(self.data_dir, 'MinPrep.yaml'),
                     os.path.join(self.data_dir, 'NoPrep.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(2), self._make_test_graph(1)]
        expected = "MinPrep.yaml"

        self._prepare_run_preprocessing()
        self._check_run_pipeline_fallback(self.interface._run_preprocessing, pipe_list, graph_list, expected)

    def test_run_preprocessing_fallback_2failof3(self):
        pipe_list = [os.path.join(self.data_dir, 'Preprocess.yaml'),
                     os.path.join(self.data_dir, 'MinPrep.yaml'),
                     os.path.join(self.data_dir, 'NoPrep.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(0), self._make_test_graph(1)]
        expected = "NoPrep.yaml"

        self._prepare_run_preprocessing()
        self._check_run_pipeline_fallback(self.interface._run_preprocessing, pipe_list, graph_list, expected)

    def test_run_preprocessing_fallback_2failof3_inverse(self):
        pipe_list = [os.path.join(self.data_dir, 'Preprocess.yaml'),
                     os.path.join(self.data_dir, 'MinPrep.yaml'),
                     os.path.join(self.data_dir, 'NoPrep.yaml')]
        graph_list = [self._make_test_graph(0), self._make_test_graph(2), self._make_test_graph(0)]
        expected = "MinPrep.yaml"

        self._prepare_run_preprocessing()
        self._check_run_pipeline_fallback(self.interface._run_preprocessing, pipe_list, graph_list, expected)

    def test_get_output_run(self):
        filename = "ApPipe.yaml"
        date = "2023-01-22"
        out_chain = self.interface._get_output_chain(date)
        self.assertEqual(out_chain, f"{instname}/prompt/output-2023-01-22")
        preload_run = self.interface._get_preload_run(date)
        self.assertEqual(preload_run, f"{instname}/prompt/output-2023-01-22/Preload/{self.deploy_id}")
        out_run = self.interface._get_output_run(filename, date)
        self.assertEqual(out_run, f"{instname}/prompt/output-2023-01-22/ApPipe/{self.deploy_id}")
        init_run = self.interface._get_init_output_run(filename, date)
        self.assertEqual(init_run, f"{instname}/prompt/output-2023-01-22/ApPipe/{self.deploy_id}")

    def test_get_template_types(self):
        template_types = self.interface._get_template_types()
        self.assertEqual(template_types, {"goodSeeingCoadd"})

    def _assert_in_collection(self, butler, collection, dataset_type, data_id):
        # Pass iff any dataset matches the query, no need to check them all.
        for dataset in butler.registry.queryDatasets(dataset_type, collections=collection, dataId=data_id):
            return
        self.fail(f"No datasets found matching {dataset_type}@{data_id} in {collection}.")

    def _assert_not_in_collection(self, butler, collection, dataset_type, data_id):
        # Fail iff any dataset matches the query, no need to check them all.
        for dataset in butler.registry.queryDatasets(dataset_type, collections=collection, dataId=data_id):
            self.fail(f"{dataset} matches {dataset_type}@{data_id} in {collection}.")

    def test_clean_local_repo(self):
        """Test that clean_local_repo removes old datasets from the datastore.
        """
        # Safe to define custom dataset types and IDs, because the repository
        # is regenerated for each test.
        butler = self.interface.butler
        raw_data_id, _ = fake_file_data("foo.bar",
                                        butler.dimensions,
                                        self.interface.instrument,
                                        self.next_visit)
        calib_data_id_1 = {k: v for k, v in raw_data_id.required.items() if k in {"instrument", "detector"}}
        calib_data_id_2 = {"instrument": self.interface.instrument.getName(), "detector": 1}
        calib_data_id_3 = {"instrument": self.interface.instrument.getName(), "detector": 2}
        calib_data_id_4 = {"instrument": self.interface.instrument.getName(), "detector": 3}
        processed_data_id = {(k if k != "exposure" else "visit"): v for k, v in raw_data_id.required.items()}
        butler_tests.addDataIdValue(butler, "exposure", raw_data_id["exposure"])
        butler_tests.addDataIdValue(butler, "visit", processed_data_id["visit"])
        butler_tests.addDatasetType(butler, "raw", raw_data_id.required.keys(), "Exposure")
        butler_tests.addDatasetType(butler, "src", processed_data_id.keys(), "SourceCatalog")
        butler_tests.addDatasetType(butler, "calexp", processed_data_id.keys(), "ExposureF")
        butler_tests.addDatasetType(butler, "bias", calib_data_id_1.keys(), "ExposureF")

        exp = lsst.afw.image.ExposureF(20, 20)
        cat = lsst.afw.table.SourceCatalog()
        # Since we're not calling prep_butler, need to set up the collections by hand
        raw_collection = self.interface.instrument.makeDefaultRawIngestRunName()
        butler.registry.registerCollection(raw_collection, CollectionType.RUN)
        out_collection = self.interface._get_output_run("ApPipe.yaml", self.interface._day_obs)
        butler.registry.registerCollection(out_collection, CollectionType.RUN)
        calib_collection = self.interface.instrument.makeCalibrationCollectionName()
        butler.registry.registerCollection(calib_collection, CollectionType.RUN)
        chain = self.interface.instrument.makeUmbrellaCollectionName()
        butler.registry.registerCollection(chain, CollectionType.CHAINED)
        butler.registry.setCollectionChain(chain, [out_collection, raw_collection, calib_collection])

        butler.put(exp, "raw", raw_data_id, run=raw_collection)
        butler.put(cat, "src", processed_data_id, run=out_collection)
        butler.put(exp, "calexp", processed_data_id, run=out_collection)
        bias_1 = butler.put(exp, "bias", calib_data_id_1, run=calib_collection)
        bias_2 = butler.put(exp, "bias", calib_data_id_2, run=calib_collection)
        bias_3 = butler.put(exp, "bias", calib_data_id_3, run=calib_collection)
        bias_4 = butler.put(exp, "bias", calib_data_id_4, run=calib_collection)
        with self.assertWarns(RuntimeWarning):  # Deliberately overflowing cache
            self.local_cache.update([bias_1, bias_2, bias_3, bias_4, ])
        self._assert_in_collection(butler, "*", "raw", raw_data_id)
        self._assert_in_collection(butler, "*", "src", processed_data_id)
        self._assert_in_collection(butler, "*", "calexp", processed_data_id)
        self._assert_in_collection(butler, "*", "bias", calib_data_id_1)
        self._assert_in_collection(butler, "*", "bias", calib_data_id_2)
        self._assert_in_collection(butler, "*", "bias", calib_data_id_3)

        self.interface.clean_local_repo({raw_data_id["exposure"]})
        self._assert_not_in_collection(butler, "*", "raw", raw_data_id)
        self._assert_not_in_collection(butler, "*", "src", processed_data_id)
        self._assert_not_in_collection(butler, "*", "calexp", processed_data_id)
        # Default cache has size 2, so one of the biases should have been removed
        self._check_cache_vs_collection(butler, self.local_cache, bias_1)
        self._check_cache_vs_collection(butler, self.local_cache, bias_2)
        self._check_cache_vs_collection(butler, self.local_cache, bias_3)

    def _check_cache_vs_collection(self, butler, cache, ref):
        if ref in cache:
            self._assert_in_collection(butler, "*", ref.datasetType, ref.dataId)
        else:
            self._assert_not_in_collection(butler, "*", ref.datasetType, ref.dataId)

    @staticmethod
    def _make_expanded_ref(registry, dtype, data_id, run):
        """Make an expanded dataset ref with the given dataset type, data ID,
        and run, and the corresponding dimension records.
        """
        return lsst.daf.butler.DatasetRef(registry.getDatasetType(dtype), data_id, run=run) \
            .expanded(registry.expandDataId(data_id))

    @staticmethod
    def _make_test_graph(n_quanta):
        """Make a `QuantumGraph` for tests.

        Parameters
        ----------
        n_quanta : `int`
            Number of quanta in a graph.

        Returns
        -------
        qgraph : `QuantumGraph`
            Quantum graph instance.
        """
        if n_quanta == 0:
            return QuantumGraph({}, universe=DimensionUniverse())
        elif n_quanta < 0:
            raise RuntimeError("Invalid input")
        else:
            with tempfile.TemporaryDirectory() as tmpdir:
                return makeSimpleQGraph(n_quanta, root=tmpdir)[1]

    def test_get_sasquatch_dispatcher(self):
        self.assertIsNone(_get_sasquatch_dispatcher())
        with unittest.mock.patch.dict(os.environ,
                                      {"SASQUATCH_URL": "https://localhost/dummy",
                                       }):
            self.assertIsNotNone(_get_sasquatch_dispatcher())

    def test_filter_datasets(self):
        """Test that _filter_datasets provides the correct values.
        """
        # Much easier to create DatasetRefs with a real repo.
        registry = self.central_butler.registry
        data1 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 5},
                                        "dummy")
        data2 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 0},
                                        "dummy")
        data3 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 1},
                                        "dummy")

        combinations = [{data1, data2}, {data1, data2, data3}]
        src_butler = unittest.mock.Mock()
        existing_butler = unittest.mock.Mock()
        # Case where src is empty now covered in test_filter_datasets_nosrc.
        for src, existing in itertools.product(combinations, [set()] + combinations):
            diff = src - existing

            def query(butler, _label):
                if butler is src_butler:
                    return src
                elif butler is existing_butler:
                    return existing
                else:
                    raise ValueError("Unknown butler!")

            with self.subTest(src=sorted(ref.dataId["detector"] for ref in src),
                              existing=sorted(ref.dataId["detector"] for ref in existing)):
                result = set(_filter_datasets(src_butler, existing_butler, query))
                self.assertEqual(result, diff)

    def test_filter_datasets_nosrc(self):
        """Test that _filter_datasets reports if the datasets are missing from
        the source repository, regardless of whether they are present in the
        destination repository.
        """
        # Much easier to create DatasetRefs with a real repo.
        registry = self.central_butler.registry
        data1 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 1},
                                        "dummy")

        src_butler = unittest.mock.Mock()
        existing_butler = unittest.mock.Mock()
        for existing in [set(), {data1}]:

            def query(butler, _label):
                if butler is src_butler:
                    return set()
                elif butler is existing_butler:
                    return existing
                else:
                    raise ValueError("Unknown butler!")

            with self.subTest(existing=sorted(ref.dataId["detector"] for ref in existing)):
                with self.assertRaises(_MissingDatasetError):
                    _filter_datasets(src_butler, existing_butler, query)

    def test_filter_datasets_all_callback(self):
        """Test that _filter_datasets passes the correct values to its callback.
        """
        def test_function(expected, incoming):
            self.assertEqual(expected, incoming)

        # Much easier to create DatasetRefs with a real repo.
        registry = self.central_butler.registry
        data1 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 5},
                                        "dummy")
        data2 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 0},
                                        "dummy")
        data3 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 1},
                                        "dummy")

        combinations = [{data1, data2}, {data1, data2, data3}]
        src_butler = unittest.mock.Mock()
        existing_butler = unittest.mock.Mock()
        # Case where src is empty covered below.
        for src, existing in itertools.product(combinations, [set()] + combinations):
            def query(butler, _label):
                if butler is src_butler:
                    return src
                elif butler is existing_butler:
                    return existing
                else:
                    raise ValueError("Unknown butler!")

            with self.subTest(src=sorted(ref.dataId["detector"] for ref in src),
                              existing=sorted(ref.dataId["detector"] for ref in existing)):
                _filter_datasets(src_butler, existing_butler, query,
                                 all_callback=functools.partial(test_function, src))

        # Should not call

        def non_callable(_):
            self.fail("Callback called during _MissingDatasetError.")

        for existing in [set()] + combinations:
            def query(butler, _label):
                if butler is src_butler:
                    return set()
                elif butler is existing_butler:
                    return existing
                else:
                    raise ValueError("Unknown butler!")

            with self.subTest(existing=sorted(ref.dataId["detector"] for ref in existing)):
                with self.assertRaises(_MissingDatasetError):
                    _filter_datasets(src_butler, existing_butler, query, all_callback=non_callable)

    def test_generic_query(self):
        # Much easier to create DatasetRefs with a real repo.
        registry = self.central_butler.registry
        data1 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 5},
                                        "dummy")
        data2 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 0},
                                        "dummy")
        data3 = self._make_expanded_ref(registry, "bias", {"instrument": "LSSTComCamSim", "detector": 1},
                                        "dummy")
        refs = [data1, data2, data3]

        butler = unittest.mock.Mock(**{"query_datasets.return_value": refs})
        result = _generic_query(["bias"], instrument="LSSTComCamSim")(butler)

        butler.query_datasets.assert_called_once()
        self.assertEqual(butler.query_datasets.call_args.args, ("bias", ))
        # Implementation may add other kwargs.
        self.assertEqual(butler.query_datasets.call_args.kwargs["instrument"], "LSSTComCamSim")
        self.assertEqual(set(result), set(refs))

    def test_generic_query_nodim(self):
        """Test that _generic_query provides the correct values when
        a repository is missing not only datasets, but the dimensions
        to define them.
        """
        butler = unittest.mock.Mock(**{
            "query_datasets.side_effect": lsst.daf.butler.registry.DataIdValueError(
                f"Unknown values specified for governor dimension instrument: {instname}")
        })
        result = _generic_query(["bias"], instrument=instname)(butler)

        self.assertEqual(result, set())

    def test_filter_calibs_by_date_valid(self):
        # _filter_calibs_by_date requires a collection, not merely an iterable
        all_calibs = list(self.central_butler.registry.queryDatasets("bias"))
        now_calibs = list(_filter_calibs_by_date(
            self.central_butler, "LSSTComCamSim/calib", all_calibs,
            astropy.time.Time("2024-06-20 00:00:00", scale="utc")
        ))
        self.assertEqual(len(now_calibs), 2)
        for calib in now_calibs:
            self.assertEqual(calib.run, "u/jchiang/bias_70240217_w_2024_07/20240218T190659Z")

    def test_filter_calibs_by_date_never(self):
        # _filter_calibs_by_date requires a collection, not merely an iterable
        all_calibs = list(self.central_butler.registry.queryDatasets("bias"))
        with warnings.catch_warnings():
            # Avoid "dubious year" warnings from using a 2050 date
            warnings.simplefilter("ignore", category=erfa.ErfaWarning)
            future_calibs = list(_filter_calibs_by_date(
                self.central_butler, "LSSTComCamSim/calib", all_calibs,
                astropy.time.Time("2050-01-01 00:00:00", scale="utc")
            ))
        self.assertEqual(len(future_calibs), 0)

    def test_filter_calibs_by_date_unbounded(self):
        # _filter_calibs_by_date requires a collection, not merely an iterable
        all_calibs = set(self.central_butler.registry.queryDatasets(["camera", "transmission_filter"]))
        valid_calibs = set(_filter_calibs_by_date(
            self.central_butler, "LSSTComCamSim/calib", all_calibs,
            astropy.time.Time("2015-03-15 00:00:00", scale="utc")
        ))
        self.assertEqual(valid_calibs, all_calibs)

    def test_filter_calibs_by_date_empty(self):
        valid_calibs = set(_filter_calibs_by_date(
            self.central_butler, "LSSTComCamSim/calib", [],
            astropy.time.Time("2015-03-15 00:00:00", scale="utc")
        ))
        self.assertEqual(len(valid_calibs), 0)


class MiddlewareInterfaceWriteableTest(unittest.TestCase):
    """Test the MiddlewareInterface class with faked data.

    This class creates a fresh test repository for writing to. This means test
    setup takes longer than for MiddlewareInterfaceTest, so it should be
    used sparingly.
    """
    def _create_copied_repo(self):
        """Create a fresh repository that's a copy of the test data.

        This method sets self.central_repo and arranges cleanup; cleanup would
        be awkward if this method returned a Butler instead.
        """
        # Copy test data to fresh Butler to allow write tests.
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        data_repo = os.path.join(data_dir, "central_repo")
        data_butler = Butler(data_repo, writeable=False)
        self.central_repo = tempfile.TemporaryDirectory()
        # TemporaryDirectory warns on leaks
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, self.central_repo)

        # Butler.transfer_from can't easily copy collections, so use
        # export/import instead.
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as export_file:
            with data_butler.export(filename=export_file.name) as export:
                export.saveDatasets(data_butler.registry.queryDatasets(..., collections=...))
                for collection in data_butler.registry.queryCollections():
                    export.saveCollection(collection)
            dimension_config = data_butler.dimensions.dimensionConfig
            central_butler = Butler(Butler.makeRepo(self.central_repo.name, dimensionConfig=dimension_config),
                                    writeable=True,
                                    )
            central_butler.import_(directory=data_repo, filename=export_file.name, transfer="auto")

    def setUp(self):
        self._create_copied_repo()
        central_butler = Butler(self.central_repo.name,
                                instrument=instname,
                                skymap=skymap_name,
                                collections=[f"{instname}/defaults"],
                                writeable=True)
        data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "data")
        self.input_data = os.path.join(data_dir, "input_data")

        local_repo = make_local_repo(tempfile.gettempdir(), central_butler, instname)
        self.local_cache = DatasetCache(2, {"uw_stars_20240524": 10, "goodSeeingCoadd": 30})
        second_local_repo = make_local_repo(tempfile.gettempdir(), central_butler, instname)
        self.second_local_cache = DatasetCache(2, {"uw_stars_20240524": 10, "goodSeeingCoadd": 30})
        # TemporaryDirectory warns on leaks; addCleanup also keeps the TD from
        # getting garbage-collected.
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, local_repo)
        self.addCleanup(tempfile.TemporaryDirectory.cleanup, second_local_repo)

        config = ApdbSql.init_database(db_url=f"sqlite:///{local_repo.name}/apdb.db")
        config_file = tempfile.NamedTemporaryFile(suffix=".py")
        self.addCleanup(config_file.close)
        config.save(config_file.name)

        env_patcher = unittest.mock.patch.dict(os.environ,
                                               {"CONFIG_APDB": config_file.name,
                                                })
        env_patcher.start()
        self.addCleanup(env_patcher.stop)

        # coordinates from OR4 visit 7024061700046
        ra = 215.82729413263485
        dec = -12.4705546590231
        rot = 149.86873311284756
        self.next_visit = FannedOutVisit(instrument=instname,
                                         detector=4,
                                         groupId="1",
                                         nimages=1,
                                         filters=filter,
                                         coordinateSystem=FannedOutVisit.CoordSys.ICRS,
                                         position=[ra, dec],
                                         startTime=1718661950.0,
                                         rotationSystem=FannedOutVisit.RotSys.SKY,
                                         cameraAngle=rot,
                                         survey="SURVEY",
                                         salIndex=42,
                                         scriptSalIndex=42,
                                         dome=FannedOutVisit.Dome.OPEN,
                                         duration=35.0,
                                         totalCheckpoints=1,
                                         private_sndStamp=1718661900.716517500,
                                         )
        self.logger_name = "lsst.activator.middleware_interface"

        # Populate repository.
        self.interface = MiddlewareInterface(central_butler, self.input_data, self.next_visit,
                                             pre_pipelines_full, pipelines, skymap_name, local_repo.name,
                                             self.local_cache,
                                             prefix="file://")
        self.deploy_id = self.interface._deployment
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing"):
            self.interface.prep_butler()
        filename = "fakeRawImage.fits"
        filepath = os.path.join(self.input_data, filename)
        self.raw_data_id, file_data = fake_file_data(filepath,
                                                     self.interface.butler.dimensions,
                                                     self.interface.instrument,
                                                     self.next_visit)
        self.group_data_id = {(k if k != "exposure" else "group"): (v if k != "exposure" else str(v))
                              for k, v in self.raw_data_id.required.items()}

        self.second_visit = dataclasses.replace(self.next_visit, groupId="2")
        self.second_data_id, second_file_data = fake_file_data(filepath,
                                                               self.interface.butler.dimensions,
                                                               self.interface.instrument,
                                                               self.second_visit)
        self.second_group_data_id = {(k if k != "exposure" else "group"): (v if k != "exposure" else str(v))
                                     for k, v in self.second_data_id.required.items()}
        self.second_interface = MiddlewareInterface(
            central_butler, self.input_data, self.second_visit, pre_pipelines_full, pipelines,
            skymap_name, second_local_repo.name, self.second_local_cache, prefix="file://")
        with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing"):
            self.second_interface.prep_butler()
        date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-12)))
        self.output_chain = f"{instname}/prompt/output-{date.year:04d}-{date.month:02d}-{date.day:02d}"
        self.preprocessing_run = f"{instname}/prompt/output-{date.year:04d}-{date.month:02d}-{date.day:02d}" \
                                 f"/Preprocess/{self.deploy_id}"
        self.output_run = f"{instname}/prompt/output-{date.year:04d}-{date.month:02d}-{date.day:02d}" \
                          f"/ApPipe/{self.deploy_id}"

        with unittest.mock.patch.object(self.interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = file_data
            self.interface.ingest_image(filename)
        with unittest.mock.patch.object(self.second_interface.rawIngestTask, "extractMetadata") as mock:
            mock.return_value = second_file_data
            self.second_interface.ingest_image(filename)
        self.interface.define_visits.run([self.raw_data_id])
        self.second_interface.define_visits.run([self.second_data_id])

        self._simulate_run()

    def _simulate_run(self):
        """Create a mock pipeline execution that stores a calexp for self.raw_data_id.
        """
        cat = astropy.table.Table()
        exp = lsst.afw.image.ExposureF(20, 20)
        self.processed_data_id = {(k if k != "exposure" else "visit"): v
                                  for k, v in self.raw_data_id.required.items()}
        self.second_processed_data_id = {(k if k != "exposure" else "visit"): v
                                         for k, v in self.second_data_id.required.items()}
        # Dataset types defined for local Butler on pipeline run, but code
        # assumes output types already exist in central repo.
        butler_tests.addDatasetType(self.interface.central_butler, "promptPreload_metrics",
                                    {"instrument", "group", "detector"},
                                    "MetricMeasurementBundle")
        butler_tests.addDatasetType(self.interface.central_butler, "regionTimeInfo",
                                    {"instrument", "group", "detector"},
                                    "RegionTimeInfo")
        butler_tests.addDatasetType(self.interface.central_butler, "history_diaSource",
                                    {"instrument", "group", "detector"},
                                    "ArrowAstropy")
        butler_tests.addDatasetType(self.interface.butler, "history_diaSource",
                                    {"instrument", "group", "detector"},
                                    "ArrowAstropy")
        butler_tests.addDatasetType(self.second_interface.butler, "history_diaSource",
                                    {"instrument", "group", "detector"},
                                    "ArrowAstropy")
        butler_tests.addDatasetType(self.interface.central_butler, "calexp",
                                    {"instrument", "visit", "detector"},
                                    "ExposureF")
        butler_tests.addDatasetType(self.interface.butler, "calexp",
                                    {"instrument", "visit", "detector"},
                                    "ExposureF")
        butler_tests.addDatasetType(self.second_interface.butler, "calexp",
                                    {"instrument", "visit", "detector"},
                                    "ExposureF")
        self.interface.butler.put(cat, "history_diaSource", self.group_data_id, run=self.preprocessing_run)
        self.second_interface.butler.put(cat, "history_diaSource", self.second_group_data_id,
                                         run=self.preprocessing_run)
        self.interface.butler.put(exp, "calexp", self.processed_data_id, run=self.output_run)
        self.second_interface.butler.put(exp, "calexp", self.second_processed_data_id, run=self.output_run)

    def _count_datasets(self, butler, types, collections):
        return len(set(butler.registry.queryDatasets(types, collections=collections)))

    def _count_datasets_with_id(self, butler, types, collections, data_id):
        return len(set(butler.registry.queryDatasets(types, collections=collections, dataId=data_id)))

    def test_flush_local_repo(self):
        central_butler = Butler(self.central_repo.name, writeable=True)
        # Exposure is defined by the local repo, not the central repo.
        butler_tests.addDatasetType(central_butler, "testData", {"instrument", "exposure", "detector"}, "int")
        # Implementation detail: flush_local_repo looks for output-like
        # collections to avoid transferring inputs.
        date = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-12)))
        run = f"{instname}/prompt/output-{date.year:04d}-{date.month:02d}-{date.day:02d}/" \
              f"NoPipe/{self.deploy_id}"

        dimension_config = central_butler.dimensions.dimensionConfig
        # Need to clean up the directory iff the method fails
        repo_dir = tempfile.mkdtemp()
        try:
            butler = Butler(Butler.makeRepo(repo_dir, dimensionConfig=dimension_config), writeable=True)
            instrument = self.interface.instrument
            instrument.register(butler.registry)
            butler_tests.addDataIdValue(butler, "day_obs", 20240627)
            butler_tests.addDataIdValue(butler, "group", "42")
            butler_tests.addDataIdValue(butler, "exposure", 42, physical_filter=filter)
            butler_tests.addDatasetType(butler, "testData", {"instrument", "exposure", "detector"}, "int")
            butler.registry.registerCollection(run, CollectionType.RUN)
            butler.put(42, "testData", run=run, instrument=instname, exposure=42, detector=0)

            flush_local_repo(repo_dir, central_butler)
            self.assertTrue(central_butler.exists("testData", collections=run,
                                                  instrument=instname, exposure=42, detector=0))
            self.assertFalse(os.path.exists(repo_dir))
        except Exception:
            shutil.rmtree(repo_dir, ignore_errors=True)
            raise

    def test_extra_collection(self):
        """Test that extra collections in the chain will not lead to MissingCollectionError
        even if they do not carry useful data.
        """
        central_butler = Butler(self.central_repo.name, writeable=True)
        central_butler.registry.registerCollection("emptyrun", CollectionType.RUN)
        central_butler.collections.prepend_chain("refcats", ["emptyrun"])

        # Avoid collisions with other calls to prep_butler
        with make_local_repo(tempfile.gettempdir(), central_butler, instname) as local_repo:
            interface = MiddlewareInterface(central_butler, self.input_data,
                                            dataclasses.replace(self.next_visit, groupId="42"),
                                            pre_pipelines_empty, pipelines, skymap_name, local_repo,
                                            DatasetCache(3, {"uw_stars_20240524": 10, "goodSeeingCoadd": 30}),
                                            prefix="file://")
            with unittest.mock.patch("activator.middleware_interface.MiddlewareInterface._run_preprocessing"):
                interface.prep_butler()

            self.assertEqual(
                self._count_datasets(interface.butler, "uw_stars_20240524", f"{instname}/defaults"),
                2)
            self.assertIn(
                "emptyrun",
                interface.butler.registry.queryCollections("refcats", flattenChains=True))

    def test_export_outputs(self):
        self.interface.export_outputs({self.raw_data_id["exposure"]})
        self.second_interface.export_outputs({self.second_data_id["exposure"]})

        central_butler = Butler(self.central_repo.name, writeable=False)
        self.assertEqual(self._count_datasets(central_butler, "history_diaSource", self.preprocessing_run), 2)
        self.assertEqual(self._count_datasets(central_butler, "history_diaSource", self.output_run), 0)
        self.assertEqual(self._count_datasets(central_butler, "history_diaSource", self.output_chain), 2)
        self.assertEqual(self._count_datasets(central_butler, "calexp", self.preprocessing_run), 0)
        self.assertEqual(self._count_datasets(central_butler, "calexp", self.output_run), 2)
        self.assertEqual(self._count_datasets(central_butler, "calexp", self.output_chain), 2)
        # Should be able to look up datasets by both visit and exposure
        self.assertEqual(
            self._count_datasets_with_id(central_butler, "calexp", self.output_run, self.raw_data_id),
            1)
        self.assertEqual(
            self._count_datasets_with_id(central_butler, "calexp", self.output_run, self.second_data_id),
            1)
        self.assertEqual(
            self._count_datasets_with_id(central_butler, "calexp", self.output_run, self.processed_data_id),
            1)
        self.assertEqual(
            self._count_datasets_with_id(central_butler, "calexp", self.output_run,
                                         self.second_processed_data_id),
            1)
        # Did not export calibs or other inputs.
        self.assertEqual(
            self._count_datasets(central_butler, ["bias", "uw_stars_20240524", "skyMap", "*Coadd"],
                                 self.output_run),
            0)
        # Nothing placed in "input" collections.
        self.assertEqual(
            self._count_datasets(central_butler, ["raw", "calexp"], f"{instname}/defaults"),
            0)

    def test_compute_region(self):
        """Test preload region computation."""
        region = self.interface._compute_region()
        self.assertTrue(isinstance(region, lsst.sphgeom.Region))
        results = self.interface.butler.query_dimension_records(
            "visit_detector_region", instrument=instname, group="1"
        )
        visit_detector_region = list(results)[0].region
        # TODO: DM-47460 for a better test.
        self.assertTrue(visit_detector_region.intersects(region))
