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

import json
import os
import re
import unittest
import warnings

from lsst.resources import ResourcePath, s3utils

from shared.raw import (
    check_for_snap,
    is_path_consistent,
    get_prefix_from_snap,
    get_exp_id_from_oid,
    get_group_id_from_oid,
    LSST_REGEXP,
    IMSIM_REGEXP,
    OTHER_REGEXP,
    get_raw_path,
)
from shared.visit import FannedOutVisit

try:
    import boto3
    try:
        from moto import mock_aws
    except ImportError:
        from moto import mock_s3 as mock_aws  # Backwards-compatible with moto 4
except ImportError:
    boto3 = None


class RawBase:
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mock_aws = mock_aws()

    """Base class for raw path handling.
    """
    def setUp(self):
        super().setUp()

        self.enterContext(s3utils.clean_test_environment_for_s3())
        self.mock_aws.start()
        self.addCleanup(self.mock_aws.stop)
        s3 = boto3.resource("s3")
        self.bucket = "test-bucket-test"
        s3.create_bucket(Bucket=self.bucket)
        os.environ["IMAGE_BUCKET"] = self.bucket

        self.ra = 134.5454
        self.dec = -65.3261
        self.rot = 135.0
        self.survey = "IMAGINARY"
        self.visit = FannedOutVisit(
            instrument=self.instrument,
            detector=self.detector,
            groupId=self.group,
            nimages=self.snaps,
            filters=self.filter,
            coordinateSystem=FannedOutVisit.CoordSys.ICRS,
            position=[self.ra, self.dec],
            startTime=1_674_516_900.0,
            rotationSystem=FannedOutVisit.RotSys.SKY,
            cameraAngle=self.rot,
            survey=self.survey,
            salIndex=42,
            scriptSalIndex=42,
            dome=FannedOutVisit.Dome.OPEN,
            duration=35.0,
            totalCheckpoints=1,
            private_sndStamp=1_674_516_794.0,
        )

    def test_snap(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        assert is_path_consistent(path, self.visit)


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
class LsstBase(RawBase):
    def setUp(self):
        self.group = "2022-03-21T00:01:00"
        self.snaps = 2
        self.filter = "k2022"
        super().setUp()

    def test_snap_matching(self):
        """Test that a JSON file can be used to match an image path with a
        group.
        """
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        fits_path = ResourcePath(f"s3://{self.bucket}").join(path)
        json_path = fits_path.updatedExtension("json")
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", "S3 does not support flushing objects", UserWarning)
            with json_path.open("w") as f:
                json.dump(dict(GROUPID=self.group, CURINDEX=self.snap + 1), f)
        assert is_path_consistent(path, self.visit)
        assert get_group_id_from_oid(path) == self.group

    def test_writeread(self):
        """Test that raw module can parse the paths it creates.
        """
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        parsed = re.match(LSST_REGEXP, path)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['instrument'], str(self.instrument))
        self.assertEqual(get_exp_id_from_oid(path), self.exposure)

    def test_get_prefix(self):
        """Test that get_prefix_from_snap returns None for LSST cameras."""
        prefix = get_prefix_from_snap(self.instrument, self.group, self.detector, self.snap)
        self.assertIsNone(prefix)

    def test_check_for_snap_present(self):
        microservice = "http://fake_host/fake_app"
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        message = {"error": False, "present": True, "uri": f"s3://{self.bucket}/{path}"}

        fits_path = ResourcePath(f"s3://{self.bucket}").join(path)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", "S3 does not support flushing objects", UserWarning)
            with fits_path.open("wb"):
                pass  # Empty file is just fine

        with unittest.mock.patch("requests.get", **{"return_value.json.return_value": message}):
            oid = check_for_snap(boto3.client("s3"),
                                 self.bucket,
                                 instrument=self.instrument,
                                 microservice=microservice,
                                 group=self.group,
                                 snap=self.snap,
                                 detector=self.detector,
                                 )
        self.assertEqual(oid, path)

    def test_check_for_snap_noservice(self):
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        fits_path = ResourcePath(f"s3://{self.bucket}").join(path)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", "S3 does not support flushing objects", UserWarning)
            with fits_path.open("wb"):
                pass  # Empty file is just fine

        oid = check_for_snap(boto3.client("s3"),
                             self.bucket,
                             instrument=self.instrument,
                             microservice="",
                             group=self.group,
                             snap=self.snap,
                             detector=self.detector,
                             )
        self.assertEqual(oid, None)

    def test_check_for_snap_absent(self):
        microservice = "http://fake_host/fake_app"
        message = {"error": False, "present": False}

        with unittest.mock.patch("requests.get", **{"return_value.json.return_value": message}):
            oid = check_for_snap(boto3.client("s3"),
                                 self.bucket,
                                 instrument=self.instrument,
                                 microservice=microservice,
                                 group=self.group,
                                 snap=self.snap,
                                 detector=self.detector,
                                 )
        self.assertEqual(oid, None)

    def test_check_for_snap_error(self):
        microservice = "http://fake_host/fake_app"
        error_msg = "Microservice on strike"
        message = {"error": True, "message": error_msg}

        with unittest.mock.patch("requests.get", **{"return_value.json.return_value": message}), \
                self.assertLogs(level="WARNING") as recorder:
            oid = check_for_snap(boto3.client("s3"),
                                 self.bucket,
                                 instrument=self.instrument,
                                 microservice=microservice,
                                 group=self.group,
                                 snap=self.snap,
                                 detector=self.detector,
                                 )
        self.assertEqual(oid, None)
        self.assertTrue(any(error_msg in line for line in recorder.output))


class LatissTest(LsstBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "LATISS"
        self.detector = 0
        self.detector_name = "R00_S00"
        self.snap = 0
        self.exposure = 2022032100002
        super().setUp()

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "LATISS/20220321/AT_O_20220321_000002/AT_O_20220321_000002_R00_S00.fits"
        )


class LsstComCamTest(LsstBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "LSSTComCam"
        self.detector = 4
        self.detector_name = "R22_S11"
        self.snap = 1
        self.exposure = 2022032100003
        super().setUp()

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "LSSTComCam/20220321/CC_O_20220321_000003/CC_O_20220321_000003_R22_S11.fits"
        )


class LsstCamTest(LsstBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "LSSTCam"
        self.detector = 42
        self.detector_name = "R11_S20"
        self.snap = 0
        self.exposure = 2022032100004
        super().setUp()

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "LSSTCam/20220321/MC_O_20220321_000004/MC_O_20220321_000004_R11_S20.fits"
        )


class LsstCamImSimTest(LsstBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "LSSTCam-imSim"
        self.group = "2022-03-21T00:00:00.000088"
        self.exposure = 2080088
        self.detector = 42
        self.snap = 1
        super().setUp()

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group,
                            self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "LSSTCam-imSim/20220321/2080088/2080088_k2022_42.fits"
        )

    def test_writeread(self):
        """Test that raw module can parse the paths it creates.
        """
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        parsed = re.match(IMSIM_REGEXP, path)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['instrument'], str(self.instrument))
        self.assertEqual(parsed['expid'], str(self.exposure))
        self.assertEqual(parsed['detector'], str(self.detector))
        self.assertEqual(get_exp_id_from_oid(path), self.exposure)


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
class HscTest(RawBase, unittest.TestCase):
    def setUp(self):
        self.instrument = "HSC"
        self.group = "2022032100001"
        self.snaps = 1
        self.filter = "k2022"
        self.detector = 42
        self.snap = 0
        self.exposure = 404
        super().setUp()

    def test_writeread(self):
        """Test that raw module can parse the paths it creates.
        """
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        parsed = re.match(OTHER_REGEXP, path)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['instrument'], str(self.instrument))
        self.assertEqual(parsed['detector'], str(self.detector))
        self.assertEqual(parsed['group'], str(self.group))
        self.assertEqual(parsed['snap'], str(self.snap))
        self.assertEqual(parsed['expid'], str(self.exposure))
        self.assertEqual(parsed['filter'], str(self.filter))
        self.assertEqual(get_exp_id_from_oid(path), self.exposure)

    def test_get_raw_path(self):
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        self.assertEqual(
            path,
            "HSC/42/2022032100001/0/404/k2022/HSC-2022032100001-0-404-k2022-42.fz"
        )

    def test_get_prefix(self):
        """Test that get_prefix_from_snap returns proper prefix."""
        prefix = get_prefix_from_snap(self.instrument, self.group, self.detector, self.snap)
        self.assertEqual(prefix, "HSC/42/2022032100001/0/")

    def test_check_for_snap_present(self):
        path = get_raw_path(self.instrument, self.detector, self.group, self.snap, self.exposure, self.filter)
        fits_path = ResourcePath(f"s3://{self.bucket}").join(path)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", "S3 does not support flushing objects", UserWarning)
            with fits_path.open("wb"):
                pass  # Empty file is just fine

        oid = check_for_snap(boto3.client("s3"),
                             self.bucket,
                             instrument=self.instrument,
                             microservice="",
                             group=self.group,
                             snap=self.snap,
                             detector=self.detector,
                             )
        self.assertEqual(oid, "HSC/42/2022032100001/0/404/k2022/HSC-2022032100001-0-404-k2022-42.fz")

    def test_check_for_snap_absent(self):
        oid = check_for_snap(boto3.client("s3"),
                             self.bucket,
                             instrument=self.instrument,
                             microservice="",
                             group=self.group,
                             snap=self.snap,
                             detector=self.detector,
                             )
        self.assertEqual(oid, None)
