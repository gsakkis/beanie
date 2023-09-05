from beanie.migrations.controllers.free_fall import free_fall_migration
from beanie.migrations.controllers.iterative import iterative_migration
from beanie.odm.actions import (
    After,
    Before,
    Delete,
    Insert,
    Replace,
    Save,
    SaveChanges,
    Update,
    ValidateOnSave,
    after_event,
    before_event,
)
from beanie.odm.bulk import BulkWriter
from beanie.odm.custom_types import DecimalAnnotation
from beanie.odm.documents import Document
from beanie.odm.fields import (
    DeleteRules,
    Indexed,
    PydanticObjectId,
    WriteRules,
)
from beanie.odm.links import BackLink, Link
from beanie.odm.settings.timeseries import Granularity, TimeSeriesConfig
from beanie.odm.union_doc import UnionDoc
from beanie.odm.utils.init import init_beanie
from beanie.odm.views import View

DATABASE_MAJOR_VERSION = 4

__version__ = "1.21.0"
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
]
