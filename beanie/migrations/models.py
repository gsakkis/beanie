from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from pydantic import Field

from beanie.odm.documents import Document


class MigrationLog(Document):
    ts: datetime = Field(default_factory=datetime.now)
    name: str
    is_current: bool

    class Settings:
        name = "migrations_log"


class RunningDirections(str, Enum):
    FORWARD = "FORWARD"
    BACKWARD = "BACKWARD"


@dataclass
class RunningMode:
    direction: RunningDirections
    distance: int
