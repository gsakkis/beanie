from dataclasses import dataclass, field
from enum import Enum
from typing import List

from beanie.odm.fields import PydanticObjectId


@dataclass
class InspectionError:
    """Inspection error details"""

    document_id: PydanticObjectId
    error: str


class InspectionStatuses(str, Enum):
    """
    Statuses of the collection inspection
    """

    FAIL = "FAIL"
    OK = "OK"


@dataclass
class InspectionResult:
    """Collection inspection result"""

    status: InspectionStatuses = InspectionStatuses.OK
    errors: List[InspectionError] = field(default_factory=list)
