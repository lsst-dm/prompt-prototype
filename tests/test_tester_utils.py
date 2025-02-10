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

import tempfile
import unittest

import lsst.daf.butler.tests as butler_tests
import lsst.meas.base
from lsst.obs.subaru import HyperSuprimeCam
import lsst.resources.s3utils

from shared.raw import get_raw_path
from tester.utils import (
    get_last_group,
    make_exposure_id,
    make_group,
    decode_group,
    increment_group,
)

try:
    import boto3
    import botocore
    try:
        from moto import mock_aws
    except ImportError:
        from moto import mock_s3 as mock_aws  # Backwards-compatible with moto 4
except ImportError:
    boto3 = None


@unittest.skipIf(not boto3, "Warning: boto3 AWS SDK not found!")
class TesterUtilsTest(unittest.TestCase):
    """Test components in tester.
    """
    bucket_name = "testBucketName"

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.mock_aws = mock_aws()

    def setUp(self):
        self.enterContext(lsst.resources.s3utils.clean_test_environment_for_s3())
        self.mock_aws.start()
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucket_name)

        path = get_raw_path(
            "LSSTCam", 123, "2022-11-02T00:00:00.000001", 2, 2022110200001, "TestFilter"
        )
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test1')
        path = get_raw_path(
            "LSSTCam", 123, "2022-11-02T00:00:00.000002", 2, 2022110200002, "TestFilter"
        )
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test2')
        path = get_raw_path(
            "HSC", 123, "11020002", 2, 30, "TestFilter"
        )
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test3')

    def tearDown(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)
        try:
            try:
                bucket.objects.all().delete()
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    # the key was not reachable - pass
                    pass
                else:
                    raise
            finally:
                bucket = s3.Bucket(self.bucket_name)
                bucket.delete()
        finally:
            # Stop the S3 mock.
            self.mock_aws.stop()

    def test_get_last_group(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)

        last_group = get_last_group(bucket, "LSSTCam", "20221102")
        self.assertEqual(last_group, "2022-11-02T00:00:00.000002")

        # Test the case of no match
        last_group = get_last_group(bucket, "LSSTCam", "20110101")
        self.assertEqual(last_group, "2011-01-01T00:00:00.000000")

    def test_get_last_group_hsc(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)

        last_group = get_last_group(bucket, "HSC", "20241102")
        self.assertEqual(last_group, "11020002")

        # Test the case of no match
        last_group = get_last_group(bucket, "HSC", "20250101")
        self.assertEqual(last_group, "13010000")

    def test_exposure_id_hsc(self):
        group = "01110026"
        # Need a Butler registry to test IdGenerator
        with tempfile.TemporaryDirectory() as repo:
            butler = butler_tests.makeTestRepo(repo)
            HyperSuprimeCam().register(butler.registry)
            instruments = list(butler.registry.queryDimensionRecords(
                "instrument", dataId={"instrument": "HSC"}))
            self.assertEqual(len(instruments), 1)
            exp_max = instruments[0].exposure_max

            exp_id, headers = make_exposure_id("HSC", group, 0)
            butler_tests.addDataIdValue(butler, "visit", exp_id)
            data_id = butler.registry.expandDataId({"instrument": "HSC", "visit": exp_id, "detector": 111})

        str_exp_id = headers["EXP-ID"]
        self.assertEqual(str_exp_id, "HSCE%08d" % exp_id)
        # Above assertion passes if exp_id has 9+ digits, but such IDs aren't valid.
        self.assertEqual(len(str_exp_id[4:]), 8)
        self.assertLessEqual(exp_id, exp_max)
        # test that IdGenerator.unpacker_from_config does not raise
        config = lsst.meas.base.DetectorVisitIdGeneratorConfig()
        lsst.meas.base.IdGenerator.unpacker_from_config(config, data_id)

    def test_group_id_hsc_limits(self):
        # Confirm that the group ID generator works as long as advertised:
        # until the end of September 2024.
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)

        group = get_last_group(bucket, "HSC", "20250930")
        self.assertEqual(group, "21300000")
        with self.assertRaises(RuntimeError):
            get_last_group(bucket, "HSC", "20251001")


class TesterGoupIdTest(unittest.TestCase):
    """Test the utilities on the made-up group ID"""

    def test_round_trip(self):
        for day_obs, seq_num in [
            ("20230123", 456),
            ("20241231", 1000),
        ]:
            self.assertEqual(
                (day_obs, seq_num), decode_group(make_group(day_obs, seq_num))
            )

        for group_id in ["2023-01-23T00:00:00.000456", "2024-12-31T00:00:00.001000"]:
            self.assertEqual(group_id, make_group(*decode_group(group_id)))

    def test_increment_group_latiss(self):
        group_base = "2024-12-31T00:00:00.001000"
        offsets = {
            1: "2024-12-31T00:00:00.001001",
            99: "2024-12-31T00:00:00.001099",
            1345: "2024-12-31T00:00:00.002345",
        }
        for amount in offsets:
            self.assertEqual(
                offsets[amount], increment_group("LATISS", group_base, amount)
            )

    def test_increment_group_hsc(self):
        group_base = "24310100"
        offsets = {
            1: "24310101",
            99: "24310199",
            1345: "24311445",
        }
        for amount in offsets:
            self.assertEqual(
                offsets[amount], increment_group("HSC", group_base, amount)
            )
