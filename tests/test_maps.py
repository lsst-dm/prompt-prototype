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


import os
import tempfile
import unittest

import astropy
import healpy
import numpy

from shared.maps import PredicateMapHealpix


TESTDIR = os.path.abspath(os.path.dirname(__file__))


class PredicateMapHealpixTest(unittest.TestCase):
    def setUp(self):
        super().setUp()

        self.nside = 1
        self.pixels = healpy.nside2npix(self.nside)  # Should be 12
        # Healpy supports a short list of specific dtypes.
        self.array = numpy.zeros(self.pixels, dtype=numpy.uint8)
        for i in range(self.pixels):
            match i % 3:
                case 0: self.array[i] = 0
                case 1: self.array[i] = 1
                case 2: self.array[i] = 42

    # Use healpy functions as the oracle
    @staticmethod
    def _expected_value(nside, lon, lat, nest=False) -> bool | None:
        """Return the expected value of the default map.

        Parameters
        ----------
        nside : `int`
            The Healpix nside parameter.
        lon, lat : `float`
            The longitude and latitude in degrees. This method is
            coordinate-agnostic; it's up to the test case to ensure that the
            arguments are in the same coordinate system as the map.
        nest : `bool`
            If `True`, use nested pixel ordering, otherwise, ring ordering.

        Returns
        -------
        value : `bool` or `None`
            The (abstract) value stored at that position.
        """
        index = healpy.pixelfunc.ang2pix(nside=nside, theta=lon, phi=lat, nest=nest, lonlat=True)
        match index % 3:
            case 0: return False
            case 1: return True
            case 2: return None

    def test_fits_nest(self):
        with tempfile.NamedTemporaryFile(suffix=".fits") as fits:
            # The example maps we got use fits_IDL layout
            healpy.fitsfunc.write_map(fits.name, self.array, nest=True, fits_IDL=True)
            map = PredicateMapHealpix.from_fits(fits.name, null=42)
        self.assertEqual(map.coord, "C")
        coords = astropy.coordinates.SkyCoord(
            *healpy.pixelfunc.pix2ang(self.nside, range(self.pixels), nest=True, lonlat=True),
            unit="degree", frame="icrs")
        for coord in coords:
            self.assertEqual(map.at(coord),
                             self._expected_value(self.nside, coord.ra.degree, coord.dec.degree, nest=True))
        self.assertEqual(map.nside, self.nside)

    def test_fits_ring(self):
        with tempfile.NamedTemporaryFile(suffix=".fits") as fits:
            # The example maps we got use fits_IDL layout
            healpy.fitsfunc.write_map(fits.name, self.array, nest=False, coord="G", fits_IDL=True)
            map = PredicateMapHealpix.from_fits(fits.name, null=42)
        self.assertEqual(map.coord, "G")
        coords = astropy.coordinates.SkyCoord(
            *healpy.pixelfunc.pix2ang(self.nside, range(self.pixels), nest=False, lonlat=True),
            unit="degree", frame="galactic")
        for coord in coords:
            self.assertEqual(map.at(coord),
                             self._expected_value(self.nside, coord.l.degree, coord.b.degree, nest=False))
        self.assertEqual(map.nside, self.nside)

    def test_nest(self):
        map = PredicateMapHealpix(self.array, nest=True, null=42)
        self.assertEqual(map.coord, "C")
        coords = astropy.coordinates.SkyCoord(
            *healpy.pixelfunc.pix2ang(self.nside, range(self.pixels), nest=True, lonlat=True),
            unit="degree", frame="icrs")
        for coord in coords:
            self.assertEqual(map.at(coord),
                             self._expected_value(self.nside, coord.ra.degree, coord.dec.degree, nest=True))
        self.assertEqual(map.nside, self.nside)

    def test_ring(self):
        map = PredicateMapHealpix(self.array, nest=False, null=42)
        self.assertEqual(map.coord, "C")
        coords = astropy.coordinates.SkyCoord(
            *healpy.pixelfunc.pix2ang(self.nside, range(self.pixels), nest=False, lonlat=True),
            unit="degree", frame="icrs")
        for coord in coords:
            self.assertEqual(map.at(coord),
                             self._expected_value(self.nside, coord.ra.degree, coord.dec.degree, nest=False))
        self.assertEqual(map.nside, self.nside)

    def test_invalid(self):
        with self.assertRaises(ValueError):
            # Array length not a valid Healpix map
            PredicateMapHealpix(numpy.zeros(self.pixels+2), nest=False, null=42)
        with self.assertRaises(ValueError):
            PredicateMapHealpix(numpy.zeros(0), nest=True, null=42)
        with self.assertRaises(ValueError):
            PredicateMapHealpix(numpy.zeros(self.pixels, dtype=numpy.float64), nest=True, null=42)
        with self.assertRaises(ValueError):
            PredicateMapHealpix(numpy.zeros(self.pixels, dtype=numpy.void), nest=True, null=42)
        with self.assertRaises(ValueError):
            # 0 is reserved for False
            PredicateMapHealpix(self.array, nest=False, null=0)
        with self.assertRaises(ValueError):
            # 1 is reserved for True
            PredicateMapHealpix(self.array, nest=True, null=1)
        with self.assertRaises(ValueError):
            # Boolean arrays don't support 3-value logic
            PredicateMapHealpix(numpy.array(self.array, dtype=bool), nest=True, null=42)
        with self.assertRaises(ValueError):
            # No such coordinate system
            PredicateMapHealpix(self.array, nest=False, null=42, coords="A")

    def test_dtypes(self):
        for dtype in {numpy.uint8, numpy.int64, int, bool}:
            array = numpy.array(self.array, dtype=dtype)
            map = PredicateMapHealpix(array, nest=False, null=42 if dtype is not bool else None)
            self.assertEqual(map.is_ternary, dtype is not bool)
            coords = astropy.coordinates.SkyCoord(
                *healpy.pixelfunc.pix2ang(self.nside, range(self.pixels), nest=False, lonlat=True),
                unit="degree", frame="icrs")
            for coord in coords:
                expected = self._expected_value(self.nside, coord.ra.degree, coord.dec.degree, nest=False)
                if dtype is bool and expected is None:
                    expected = True  # Match array type conversion above
                self.assertEqual(map.at(coord), expected)

    def _check_coords(self, healpy_frame, astropy_frame):
        map = PredicateMapHealpix(self.array, nest=False, null=42, coords=healpy_frame)
        self.assertEqual(map.coord, healpy_frame)
        for frame in {"icrs", "galactic", "geocentricmeanecliptic"}:
            coords = astropy.coordinates.SkyCoord(
                *healpy.pixelfunc.pix2ang(self.nside, range(self.pixels), nest=False, lonlat=True),
                unit="degree", frame=frame)
            transformeds = coords.transform_to(astropy_frame)
            for coord, transformed in zip(coords, transformeds):
                # Test that the map can handle the transformation automatically.
                # Aside from the coord property, the map encapsulates its
                # underlying coordinate system so it can't be queried directly.
                self.assertEqual(map.at(coord), map.at(transformed))

    def test_equatorial(self):
        self._check_coords("C", "icrs")

    def test_galactic(self):
        self._check_coords("G", "galactic")

    def test_ecliptic(self):
        self._check_coords("E", "geocentricmeanecliptic")
