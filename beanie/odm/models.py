from dataclasses import dataclass, field
from typing import List

from beanie.odm.enums import InspectionStatuses
from beanie.odm.fields import PydanticObjectId


@dataclass
class InspectionError:
    """Inspection error details"""

    document_id: PydanticObjectId
    error: str


@dataclass
class InspectionResult:
    """Collection inspection result"""

    status: InspectionStatuses = InspectionStatuses.OK
    errors: List[InspectionError] = field(default_factory=list)
