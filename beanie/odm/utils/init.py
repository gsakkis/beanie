import importlib
import inspect
from typing import List, Optional, Type, Union

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel
from pymongo import IndexModel

import beanie
from beanie.odm.actions import ActionRegistry
from beanie.odm.documents import Document, DocumentSettings
from beanie.odm.fields import ExpressionField, IndexModelField
from beanie.odm.links import DOCS_REGISTRY
from beanie.odm.union_doc import UnionDoc, UnionDocSettings
from beanie.odm.views import View, ViewSettings

DocumentLike = Union[Document, View, UnionDoc]


def init_document(cls: Type[Document]) -> None:
    cls._settings = DocumentSettings.from_model_type(cls)
    cls._children = {}
    cls._hidden_fields = set()
    for k, v in cls.model_fields.items():
        setattr(cls, k, ExpressionField(v.alias or k))
        if isinstance(v.json_schema_extra, dict) and v.json_schema_extra.get(
            "hidden"
        ):
            cls._hidden_fields.add(k)
    ActionRegistry.init_actions(cls)

    # set up document inheritance
    parent_cls = cls.parent_document_cls()
    if cls._settings.is_root and not (
        parent_cls and parent_cls._settings.is_root
    ):
        cls._class_id = cls.__name__
    elif parent_cls and parent_cls._class_id:
        cls._settings.name = parent_cls.get_collection_name()
        cls._class_id = class_id = f"{parent_cls._class_id}.{cls.__name__}"
        while parent_cls is not None:
            parent_cls._children[class_id] = cls
            parent_cls = parent_cls.parent_document_cls()


def init_view(cls: Type[View]):
    cls._settings = ViewSettings.from_model_type(cls)
    for k, v in cls.model_fields.items():
        setattr(cls, k, ExpressionField(v.alias or k))


def init_union_doc(cls: Type[UnionDoc]):
    cls._settings = UnionDocSettings.from_model_type(cls)
    cls._children = {}


async def init_indexes(
    cls: Type[Document], allow_index_dropping: bool
) -> None:
    # Indexed field wrapped with Indexed()
    new_indexes = []
    for k, v in cls.model_fields.items():
        if indexed := getattr(v.annotation, "_indexed", None):
            index_type, kwargs = indexed
            index = IndexModel([(v.alias or k, index_type)], **kwargs)
            new_indexes.append(IndexModelField(index))

    settings = cls._settings
    merge_indexes = IndexModelField.merge_indexes
    if settings.merge_indexes:
        super_indexes: List[IndexModelField] = []
        for superclass in reversed(cls.mro()):
            if issubclass(superclass, Document) and superclass != Document:
                if indexes := superclass._settings.indexes:
                    super_indexes = merge_indexes(super_indexes, indexes)
        new_indexes = merge_indexes(new_indexes, super_indexes)
    elif settings.indexes:
        new_indexes = merge_indexes(new_indexes, settings.indexes)

    # Only drop indexes if the user specifically allows for it
    collection = cls.get_motor_collection()
    if allow_index_dropping:
        old_indexes = IndexModelField.from_motor_index_information(
            await collection.index_information()
        )
        for index in old_indexes:
            if index not in new_indexes:
                await collection.drop_index(index.name)

    # create indices
    if new_indexes:
        await collection.create_indexes([i.index for i in new_indexes])


def register_document_model(
    type_or_str: Union[Type[DocumentLike], str]
) -> Type[DocumentLike]:
    if isinstance(type_or_str, type):
        model = type_or_str
        module = inspect.getmodule(model)
    else:
        try:
            module_name, class_name = type_or_str.rsplit(".", 1)
        except ValueError:
            raise ValueError(
                f"'{type_or_str}' doesn't have '.' path, eg. path.to.model.class"
            )
        module = importlib.import_module(module_name)
        model = getattr(module, class_name)

    for name, obj in inspect.getmembers(module):
        if inspect.isclass(obj) and issubclass(obj, BaseModel):
            DOCS_REGISTRY.register(name, obj)

    return model


def type_sort_key(doctype: Type[DocumentLike]) -> int:
    if issubclass(doctype, UnionDoc):
        return 0
    if issubclass(doctype, Document):
        return 1
    if issubclass(doctype, View):
        return 2
    assert False


async def init_beanie(
    database: Optional[AsyncIOMotorDatabase] = None,
    connection_string: Optional[str] = None,
    document_models: Optional[List[Union[Type[DocumentLike], str]]] = None,
    allow_index_dropping: bool = False,
    recreate_views: bool = False,
):
    """
    Beanie initialization

    :param database: AsyncIOMotorDatabase - motor database instance
    :param connection_string: str - MongoDB connection string
    :param document_models: List[Union[Type[DocumentLike], str]] - model classes
    or strings with dot separated paths
    :param allow_index_dropping: bool - if index dropping is allowed. Default False
    :param recreate_views: bool - if views should be recreated. Default False
    :return: None
    """
    if document_models is None:
        raise ValueError("document_models parameter must be set")

    if connection_string is database is None:
        raise ValueError("Either connection_string or database must be set")

    if connection_string is not None and database is not None:
        raise ValueError("Either connection_string or database must be set")

    if database is None:
        client = AsyncIOMotorClient(connection_string)
        database = client.get_default_database()

    build_info = await database.command({"buildInfo": 1})
    beanie.DATABASE_MAJOR_VERSION = int(build_info["version"].split(".")[0])

    models: List[Type[DocumentLike]] = []
    for document_model in document_models:
        model = register_document_model(document_model)
        if issubclass(model, Document):
            for superclass in reversed(model.mro()):
                if (
                    issubclass(superclass, Document)
                    and superclass != Document
                    and superclass not in models
                ):
                    models.append(superclass)
        elif model not in models:
            models.append(model)
    models.sort(key=type_sort_key)

    for model in models:
        if issubclass(model, UnionDoc):
            init_union_doc(model)
            await model.get_settings().update_from_database(database)
        elif issubclass(model, Document):
            init_document(model)
            await model.get_settings().update_from_database(database)
            await init_indexes(model, allow_index_dropping)
        elif issubclass(model, View):
            init_view(model)
            await model.get_settings().update_from_database(
                database, recreate=recreate_views
            )

        if hasattr(model, "custom_init"):
            await model.custom_init()
