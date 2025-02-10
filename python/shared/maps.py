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


__all__ = ["PredicateMapHealpix"]


import numbers

import astropy.coordinates
import healpy as hp
import numpy as np


class PredicateMapHealpix:
    """A healpy map that stores a boolean or ternary value at each position on
    the sky.

    Parameters
    ----------
    array : `numpy.ndarray` [integer]
        The map contents.
    nest : `bool`
        If `True`, ``array`` is indexed with nested ordering. If `False`, it's
        indexed with ring ordering.
    null : integer or `None`
        The value in ``array`` that represents no data. Cannot be 0 or 1. `None` for boolean maps.
    coords : {"C", "G", "E"}
        The coordinate system used by the map. ``"C"`` (the default) denotes
        equatorial, ``"G"`` Galactic, and ``"E"`` ecliptic.

    See Also
    --------
    from_fits
    """

    def __init__(self, array: np.ndarray,
                 *, nest: bool, null: numbers.Integral | None = None, coords: str = "C"):
        npix = len(array)
        if not npix:
            raise ValueError("Cannot index a zero-length map.")
        self._nside = hp.pixelfunc.npix2nside(npix)  # Validates npix and array
        self._map = array
        dtype = self._map.dtype
        if dtype.kind not in {"b", "i", "u"}:
            raise ValueError(f"Predicate maps must use boolean or integer data, got {dtype}.")
        self._nest = nest
        if dtype.kind == "b" and null is not None:
            raise ValueError(f"Cannot set null value for array of type {dtype}, got {null}.")
        if null in {0, 1}:
            raise ValueError(f"Null value must be distinct from true/false, got {null}.")
        self._null = null
        self._healpy_coords = coords
        match self._healpy_coords:
            case "C":
                self._astropy_coords = "icrs"
            case "G":
                self._astropy_coords = "galactic"
            case "E":
                # Healpix's coordinate systems are so vague that it probably
                # doesn't matter which ecliptic system we use.
                self._astropy_coords = "geocentricmeanecliptic"
            case _:
                raise ValueError(f"Healpy coordinates must be one of C, G, E; got {coords}.")

    @property
    def nside(self) -> int:
        """The map's resolution as the healpix nside parameter (`int`, read-only).
        """
        return self._nside

    @property
    def coord(self) -> str:
        """The map's coordinate system, in healpy notation ({"C", "G", "E"}, read-only).
        """
        return self._healpy_coords

    @property
    def is_ternary(self) -> bool:
        """Whether the map supports 3-value logic (`bool`, read-only).

        Equivalently, whether `at` can return `None` for some value.
        """
        return self._null is not None

    @classmethod
    def from_fits(cls, filename: str, hdu: int = 1, null: numbers.Integral | None = None):
        """Read a predicate map from a Healpix FITS file.

        Parameters
        ----------
        filename : `str`
            The file to load.
        hdu : `int`, optional
            The HDU to read from the file.
        null : integer
            The pixel value that represents no data. Cannot be 0 or 1.

        Returns
        -------
        map
            The map contained in ``filename``.
        """
        map, header = hp.fitsfunc.read_map(filename, dtype=None, nest=None, hdu=hdu, h=True)
        header = dict(header)

        try:
            match header["ORDERING"]:
                case "RING":
                    nest = False
                case "NESTED":
                    nest = True
                case x:
                    raise ValueError(f"Unrecognized ORDERING keyword in FITS header: {x}.")
        except KeyError as e:
            raise ValueError("FITS file does not have an ORDERING keyword.") from e
        coords = header.get("COORDSYS", "C")  # FITS files are allowed to not specify a system.
        if "INDEXSCHM" in header and header["INDEXSCHM"] != "IMPLICIT":
            raise NotImplementedError(f"Only implicit indexing is supported, got {header['INDEXSCHM']}.")
        if "OBJECT" in header and header["OBJECT"] != "FULLSKY":
            raise NotImplementedError(f"Only full-sky maps are supported, got {header['OBJECT']}.")

        return cls(map, nest=nest, null=null, coords=coords)

    def at(self, position: astropy.coordinates.SkyCoord) -> bool | None:
        """Read the value at a particular position.

        Parameters
        ----------
        position : `astropy.coordinates.SkyCoord`
            The position at which to read the map.

        Returns
        -------
        value : `bool` or `None`
            The binary or (if `is_ternary`) ternary value stored at that position.
        """
        native = position.transform_to(self._astropy_coords)
        # No, there doesn't seem to be an easier way to get frame-agnostic coordinates!
        lon = native.represent_as(astropy.coordinates.SphericalRepresentation).lon.degree
        lat = native.represent_as(astropy.coordinates.SphericalRepresentation).lat.degree
        pixel = hp.pixelfunc.ang2pix(self._nside, lon, lat, nest=self._nest, lonlat=True)
        match self._map[pixel]:
            case 1:  # This works for bool dtype because 1 == True
                return True
            case 0:
                return False
            case self._null:
                return None
            case x:
                raise RuntimeError(f"Map contains unexpected value {x} at {native}.")
