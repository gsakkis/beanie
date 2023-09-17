from beanie.migrations.controllers.free_fall import free_fall_migration
from beanie.migrations.controllers.iterative import iterative_migration
from beanie.odm.actions import (
    ActionDirections,
    EventTypes,
    after_event,
    before_event,
)
from beanie.odm.bulk import BulkWriter
from beanie.odm.custom_types import DecimalAnnotation
from beanie.odm.custom_types.bson.binary import BsonBinary
from beanie.odm.documents import DeleteRules, Document, WriteRules
from beanie.odm.fields import Indexed, PydanticObjectId
from beanie.odm.links import BackLink, Link
from beanie.odm.queries.update import UpdateResponse
from beanie.odm.timeseries import Granularity, TimeSeriesConfig
from beanie.odm.union_doc import UnionDoc
from beanie.odm.utils.init import init_beanie
from beanie.odm.views import View

DATABASE_MAJOR_VERSION = 4

Insert = EventTypes.INSERT
Replace = EventTypes.REPLACE
Save = EventTypes.SAVE
SaveChanges = EventTypes.SAVE_CHANGES
ValidateOnSave = EventTypes.VALIDATE_ON_SAVE
Delete = EventTypes.DELETE
Update = EventTypes.UPDATE
Before = ActionDirections.BEFORE
After = ActionDirections.AFTER
del EventTypes, ActionDirections

__version__ = "1.22.5"
__all__ = [
    "DATABASE_MAJOR_VERSION",
    # ODM
    "Document",
    "View",
    "UnionDoc",
    "init_beanie",
    "PydanticObjectId",
    "Indexed",
    "TimeSeriesConfig",
    "Granularity",
    # Actions
    "before_event",
    "after_event",
    "Insert",
    "Replace",
    "Save",
    "SaveChanges",
    "ValidateOnSave",
    "Delete",
    "Before",
    "After",
    "Update",
    # Bulk Write
    "BulkWriter",
    # Migrations
    "iterative_migration",
    "free_fall_migration",
    # Relations
    "Link",
    "BackLink",
    "WriteRules",
    "DeleteRules",
    # Custom Types
    "DecimalAnnotation",
    "BsonBinary",
    # UpdateResponse
    "UpdateResponse",
]
