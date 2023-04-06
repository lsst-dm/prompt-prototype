__all__ = ["FannedOutVisit"]

from dataclasses import dataclass, field
import enum


@dataclass(frozen=True, kw_only=True)
class FannedOutVisit:
    # Elements must be hashable and JSON-persistable; built-in types
    # recommended. list is not hashable, but gets special treatment because
    # neither Kafka nor JSON deserialize sequences as tuples.

    # Inherited from SAL next_visit schema; keep in sync with
    # https://ts-xml.lsst.io/sal_interfaces/ScriptQueue.html#nextvisit
    class CoordSys(enum.IntEnum):
        # This is a redeclaration of lsst.ts.idl.enums.Script.MetadataCoordSys,
        # but we need FannedOutVisit to work in code that can't import lsst.ts.
        NONE = 1
        ICRS = 2
        OBSERVED = 3
        MOUNT = 4

    class RotSys(enum.IntEnum):
        # Redeclaration of lsst.ts.idl.enums.Script.MetadataRotSys.
        NONE = 1
        SKY = 2
        HORIZON = 3
        MOUNT = 4

    class Dome(enum.IntEnum):
        # Redeclaration of lsst.ts.idl.enums.Script.MetadataDome.
        CLOSED = 1
        OPEN = 2
        EITHER = 3

    salIndex: int
    scriptSalIndex: int
    groupId: str                # observatory-specific ID; not the same as visit number
    coordinateSystem: CoordSys  # coordinate system of position
    # (ra, dec) or (az, alt) in degrees. Use compare=False to exclude from hash.
    position: list[float] = field(compare=False)
    rotationSystem: RotSys      # coordinate system of cameraAngle
    cameraAngle: float          # in degrees
    filters: str                # physical filter(s)
    dome: Dome
    duration: float             # script execution, not exposure
    nimages: int                # number of snaps expected, 0 if unknown
    survey: str                 # survey name
    totalCheckpoints: int

    # Added by the Kafka consumer at USDF.
    instrument: str             # short name
    detector: int

    def __str__(self):
        """Return a short string that disambiguates the visit but does not
        include "metadata" fields.
        """
        return f"(instrument={self.instrument}, groupId={self.groupId}, detector={self.detector})"
