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

import tempfile
import unittest

import boto3
import botocore
from moto import mock_s3

import lsst.daf.butler.tests as butler_tests
import lsst.meas.base
from lsst.obs.subaru import HyperSuprimeCam

from activator.raw import get_raw_path
from tester.utils import get_last_group, make_exposure_id


class TesterUtilsTest(unittest.TestCase):
    """Test components in tester.
    """
    mock_s3 = mock_s3()
    bucket_name = "testBucketName"

    def setUp(self):
        self.mock_s3.start()
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=self.bucket_name)

        path = get_raw_path("TestCam", 123, "2022110200001", 2, 30, "TestFilter")
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test1')
        path = get_raw_path("TestCam", 123, "2022110200002", 2, 30, "TestFilter")
        obj = s3.Object(self.bucket_name, path)
        obj.put(Body=b'test2')

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
            self.mock_s3.stop()

    def test_get_last_group(self):
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(self.bucket_name)

        last_group = get_last_group(bucket, "TestCam", "20221102")
        self.assertEqual(last_group, 2022110200002)

        # Test the case of no match
        last_group = get_last_group(bucket, "TestCam", "20110101")
        self.assertEqual(last_group, int(20110101) * 100_000)

    def test_exposure_id_hsc(self):
        group = "2023011100026"
        # Need a Butler registry to test IdGenerator
        with tempfile.TemporaryDirectory() as repo:
            butler = butler_tests.makeTestRepo(repo)
            HyperSuprimeCam().register(butler.registry)
            instruments = list(butler.registry.queryDimensionRecords(
                "instrument", dataId={"instrument": "HSC"}))
            self.assertEqual(len(instruments), 1)
            exp_max = instruments[0].exposure_max

            _, str_exp_id, exp_id = make_exposure_id("HSC", int(group), 0)
            butler_tests.addDataIdValue(butler, "visit", exp_id)
            data_id = butler.registry.expandDataId({"instrument": "HSC", "visit": exp_id, "detector": 111})

        self.assertEqual(str_exp_id, "HSCE%08d" % exp_id)
        # Above assertion passes if exp_id has 9+ digits, but such IDs aren't valid.
        self.assertEqual(len(str_exp_id[4:]), 8)
        self.assertLessEqual(exp_id, exp_max)
        # test that IdGenerator.unpacker_from_config does not raise
        config = lsst.meas.base.DetectorVisitIdGeneratorConfig()
        lsst.meas.base.IdGenerator.unpacker_from_config(config, data_id)

    def test_exposure_id_hsc_limits(self):
        # Confirm that the exposure ID generator works as long as advertised:
        # until the end of September 2024.
        _, _, exp_id = make_exposure_id("HSC", 2024093009999, 0)
        self.assertEqual(exp_id, 21309999)
        with self.assertRaises(RuntimeError):
            make_exposure_id("HSC", 2024100100000, 0)
