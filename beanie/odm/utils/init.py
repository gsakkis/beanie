import importlib
import inspect
from operator import attrgetter
from typing import List, Optional, Type, Union

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel
from pymongo import IndexModel

import beanie
from beanie.odm.actions import ActionRegistry
from beanie.odm.documents import Document
from beanie.odm.fields import ExpressionField
from beanie.odm.links import DOCS_REGISTRY, LinkedModel
from beanie.odm.settings.document import DocumentSettings, IndexModelField
from beanie.odm.settings.union_doc import UnionDocSettings
from beanie.odm.settings.view import ViewSettings
from beanie.odm.union_doc import UnionDoc
from beanie.odm.views import View

DocumentLike = Union[Document, View, UnionDoc]


def get_parent_document_cls(cls: Type[Document]) -> Optional[Type[Document]]:
    parent_cls = next(b for b in cls.__bases__ if issubclass(b, Document))
    return parent_cls if parent_cls is not Document else None


def init_document(cls: Type[Document]) -> None:
    settings = DocumentSettings.from_model_type(cls)

    parent_cls = get_parent_document_cls(cls)
    if settings.is_root and not (
        parent_cls and parent_cls.get_settings().is_root
    ):
        cls._class_id = cls.__name__
    elif parent_cls and parent_cls._class_id:
        settings.name = parent_cls.get_collection_name()
        cls._class_id = class_id = f"{parent_cls._class_id}.{cls.__name__}"
        while parent_cls is not None:
            parent_cls._children[class_id] = cls
            parent_cls = get_parent_document_cls(parent_cls)

    # register in the Union Doc
    union_doc = settings.union_doc
    if union_doc is not None:
        name = settings.name
        union_doc._children[name] = cls
        settings.name = union_doc._settings.name
        settings.union_doc_alias = name

    cls._children = {}
    cls._settings = settings
    init_fields(cls)
    cls.set_hidden_fields()
    ActionRegistry.init_actions(cls)


def init_view(cls: Type[View]):
    init_fields(cls)
    cls._settings = ViewSettings.from_model_type(cls)


def init_union_doc(cls: Type[UnionDoc]):
    cls._children = {}
    cls._settings = UnionDocSettings.from_model_type(cls)


def init_fields(cls) -> None:
    for k, v in cls.model_fields.items():
        setattr(cls, k, ExpressionField(v.alias or k))
    if issubclass(cls, LinkedModel):
        cls.init_link_fields()


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
    models.sort(key=attrgetter("_sort_order"))

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
