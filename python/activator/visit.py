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

__all__ = ["FannedOutVisit", "SummitVisit", "BareVisit"]

from dataclasses import dataclass, field, asdict
import enum

import astropy.coordinates
import astropy.units as u


@dataclass(frozen=True, kw_only=True)
class BareVisit:
    # Elements must be hashable and JSON-persistable; built-in types
    # recommended. list is not hashable, but gets special treatment because
    # neither Kafka nor JSON deserialize sequences as tuples.

    # Inherited from SAL next_visit schema; keep in sync with
    # https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html#nextvisit
    class CoordSys(enum.IntEnum):
        # This is a redeclaration of lsst.ts.xml.enums.Script.MetadataCoordSys,
        # but we need BareVisit to work in code that can't import lsst.ts.
        NONE = 1
        ICRS = 2
        OBSERVED = 3
        MOUNT = 4

    class RotSys(enum.IntEnum):
        # Redeclaration of lsst.ts.xml.enums.Script.MetadataRotSys.
        NONE = 1
        SKY = 2
        HORIZON = 3
        MOUNT = 4

    class Dome(enum.IntEnum):
        # Redeclaration of lsst.ts.xml.enums.Script.MetadataDome.
        CLOSED = 1
        OPEN = 2
        EITHER = 3

    # script queue that generated the event. One queue usually runs one telescope, but they can switch
    salIndex: int
    scriptSalIndex: int
    # Observatory-specific ID. Same as Butler's group_name, not the same as
    # Butler's group_id or visit number
    groupId: str
    coordinateSystem: CoordSys  # coordinate system of position
    # (ra, dec) or (az, alt) in degrees. Use compare=False to exclude from hash.
    position: list[float] = field(compare=False)
    startTime: float            # expected start time in TAI
    rotationSystem: RotSys      # coordinate system of cameraAngle
    cameraAngle: float          # in degrees
    # physical filter(s) name as used in Middleware. It is a combination of filter and
    # grating joined by a "~". For example, "SDSSi_65mm~empty". May be empty
    # to indicate no specific filter.
    filters: str
    dome: Dome
    duration: float             # script execution, not exposure
    nimages: int                # number of snaps expected, 0 if unknown
    instrument: str             # short name
    survey: str                 # survey name
    totalCheckpoints: int

    def __str__(self):
        """Return a short string that represents the visit but does not
        include complete metadata.
        """
        return f"(groupId={self.groupId}, survey={self.survey}, " \
               f"salIndex={self.salIndex}, instrument={self.instrument})"

    def get_boresight_icrs(self):
        """Normalize the visit position to ICRS coordinates.

        Returns
        -------
        icrs : `astropy.coordinates.SkyCoord` or `None`
            The ICRS coordinates of the position, or `None` if the visit does
            not have a position. RA is guaranteed to be normalized to
            [0, 360) degrees.

        Raises
        ------
        RuntimeError
            Raised if the coordinates are in an unsupported system.
        """
        match self.coordinateSystem:
            case BareVisit.CoordSys.NONE:
                return None
            case BareVisit.CoordSys.ICRS:
                return astropy.coordinates.SkyCoord(*self.position, unit=u.degree, frame="icrs")
            case BareVisit.CoordSys.OBSERVED:
                # Doable in principle with astropy.coordinates.AltAz
                raise RuntimeError("Alt-Az coordinates are not supported")
            case BareVisit.CoordSys.MOUNT:
                raise RuntimeError("Internal coordinates are not supported.")
            case _:
                raise RuntimeError("Unknown coordinate system %r.", self.coordinateSystem)

    def get_rotation_sky(self):
        """Normalize the visit rotation to Sky coordinates.

        Returns
        -------
        icrs : `astropy.coordinates.Angle` or `None`
            The orientation of focal +Y, measured east of north, or `None` if
            the visit does not have an orientation.

        Raises
        ------
        RuntimeError
            Raised if the rotation is in an unsupported system.
        """
        match self.rotationSystem:
            case BareVisit.RotSys.NONE:
                return None
            case BareVisit.RotSys.SKY:
                return astropy.coordinates.Angle(self.cameraAngle, unit=u.degree)
            case BareVisit.RotSys.HORIZON:
                raise RuntimeError("Alt-Az coordinates are not supported")
            case BareVisit.RotSys.MOUNT:
                raise RuntimeError("Internal coordinates are not supported.")
            case _:
                raise RuntimeError("Unknown rotation system %r.", self.rotationSystem)


@dataclass(frozen=True, kw_only=True)
class FannedOutVisit(BareVisit):
    # Extra information is added by the fan-out service at USDF.
    detector: int
    private_sndStamp: float     # time of visit publication; TAI in unix seconds

    def __str__(self):
        """Return a short string that disambiguates the visit but does not
        include "metadata" fields.
        """
        return f"(groupId={self.groupId}, survey={self.survey}, " \
               f"detector={self.detector})"

    def get_bare_visit(self):
        """Return visit-level info as a dict"""
        info = asdict(self)
        info.pop("detector")
        info.pop("private_sndStamp")
        return info


@dataclass(frozen=True, kw_only=True)
class SummitVisit(BareVisit):
    # Extra fields are in the NextVisit messages from the summit
    private_efdStamp: float = 0.0  # time of visit publication; UTC in unix seconds
    private_kafkaStamp: float = 0.0
    private_identity: str = "ScriptQueue"
    private_revCode: str = "c9aab3df"
    private_origin: int = 0
    private_seqNum: int = 0        # counts script calls since queue start. Not the same as Butler seq_num
    private_rcvStamp: float = 0.0
    private_sndStamp: float = 0.0  # time of visit publication; TAI in unix seconds
