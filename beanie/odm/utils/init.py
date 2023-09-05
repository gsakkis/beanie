import importlib
import inspect
from operator import attrgetter
from typing import List, Optional, Type, Union

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel
from pymongo import IndexModel

from beanie.exceptions import MongoDBVersionError
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


async def init_document(
    cls: Type[Document],
    database: AsyncIOMotorDatabase,
    allow_index_dropping: bool,
) -> None:
    build_info = await database.command({"buildInfo": 1})
    cls._database_major_version = int(build_info["version"].split(".")[0])
    settings = DocumentSettings.model_validate(
        cls.Settings.__dict__ if hasattr(cls, "Settings") else {}
    )
    cls._settings = settings

    cls._children = {}
    cls._parent = None
    cls._class_id = None

    bases = [b for b in cls.__bases__ if issubclass(b, Document)]
    if len(bases) > 1:
        return None

    parent = bases[0]
    if settings.is_root and (
        parent is Document or not parent.get_settings().is_root
    ):
        cls._class_id = cls.__name__
    elif parent_class_id := getattr(parent, "_class_id", None):
        settings.name = parent.get_collection_name()
        cls._class_id = class_id = f"{parent_class_id}.{cls.__name__}"
        cls._parent = parent
        ancestor: Optional[Type[Document]] = parent
        while ancestor is not None:
            ancestor._children[class_id] = cls
            ancestor = ancestor._parent

    await init_document_collection(cls, database)
    await init_indexes(cls, allow_index_dropping)
    init_fields(cls)
    cls.set_hidden_fields()
    ActionRegistry.init_actions(cls)


async def init_document_collection(
    cls: Type[Document], database: AsyncIOMotorDatabase
) -> None:
    settings = cls._settings

    # register in the Union Doc
    union_doc = settings.union_doc
    if union_doc is not None:
        name = settings.name or cls.__name__
        union_doc._children[name] = cls
        settings.name = union_doc._settings.name
        settings.union_doc_alias = name

    if settings.name is None:
        settings.name = cls.__name__

    settings.motor_collection = database[settings.name]
    timeseries = settings.timeseries
    if timeseries is not None:
        if cls._database_major_version < 5:
            raise MongoDBVersionError(
                "Timeseries are supported by MongoDB version 5 and higher"
            )
        collections = await database.list_collection_names()
        if settings.name not in collections:
            query = timeseries.build_query(settings.name)
            settings.motor_collection = await database.create_collection(
                **query
            )


async def init_view(
    cls: Type[View], database: AsyncIOMotorDatabase, recreate_views: bool
):
    """
    Init View-based class

    :param cls:
    :return:
    """
    settings = ViewSettings.from_model_type(cls, database)
    cls._settings = settings
    init_fields(cls)
    collection_names = await database.list_collection_names()
    if recreate_views or settings.name not in collection_names:
        if settings.name in collection_names:
            await cls.get_motor_collection().drop()
        await database.command(
            {
                "create": settings.name,
                "viewOn": settings.source,
                "pipeline": settings.pipeline,
            }
        )


async def init_union_doc(cls: Type[UnionDoc], database: AsyncIOMotorDatabase):
    cls._settings = UnionDocSettings.from_model_type(cls, database)
    cls._children = {}


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
            if not issubclass(superclass, Document):
                continue
            if superclass == Document:
                continue
            if indexes := superclass._settings.indexes:
                super_indexes = merge_indexes(super_indexes, indexes)
        new_indexes = merge_indexes(new_indexes, super_indexes)
    elif settings.indexes:
        new_indexes = merge_indexes(new_indexes, settings.indexes)

    # delete indexes
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

    if connection_string is not None:
        client = AsyncIOMotorClient(connection_string)
        database = client.get_default_database()

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
            await init_union_doc(model, database)
        elif issubclass(model, Document):
            await init_document(model, database, allow_index_dropping)
        elif issubclass(model, View):
            await init_view(model, database, recreate_views)

        if hasattr(model, "custom_init"):
            await model.custom_init()
