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

import json
import unittest

from activator.visit import Visit


class VisitTest(unittest.TestCase):
    """Test the Visit class's functionality.
    """
    def setUp(self):
        super().setUp()

        self.testbed = Visit(
            instrument="NotACam",
            detector=42,
            groupId="2023-01-23T23:33:14.762",
            nimages=2,
            filters="k2022",
            coordinateSystem=Visit.CoordSys.ICRS,
            position=[134.5454, -65.3261],
            rotationSystem=Visit.RotSys.SKY,
            cameraAngle=135.0,
            survey="IMAGINARY",
            salIndex=42,
            scriptSalIndex=42,
            dome=Visit.Dome.OPEN,
            duration=35.0,
            totalCheckpoints=1,
        )

    def test_hash(self):
        # Strictly speaking should test whether Visit fulfills the hash
        # contract, but it's not clear what kinds of differences the default
        # __hash__ might be insensitive to. So just test that the object
        # is hashable.
        value = hash(self.testbed)
        self.assertNotEqual(value, 0)

    def test_json(self):
        serialized = json.dumps(self.testbed.__dict__).encode("utf-8")
        deserialized = Visit(**json.loads(serialized))
        self.assertEqual(deserialized, self.testbed)
        # Test that enums are handled correctly despite being serialized as shorts.
        # isinstance checks are ambigious because IntEnum is-an int.
        self.assertIs(type(self.testbed.coordinateSystem), Visit.CoordSys)
        self.assertIs(type(deserialized.coordinateSystem), int)
        self.assertIsNot(type(deserialized.coordinateSystem), Visit.CoordSys)

    def test_str(self):
        self.assertNotEqual(str(self.testbed), repr(self.testbed))
        self.assertIn(str(self.testbed.detector), str(self.testbed))
        self.assertIn(str(self.testbed.groupId), str(self.testbed))
