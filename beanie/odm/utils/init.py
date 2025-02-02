import asyncio
import importlib
from typing import List, Optional, Sequence, Type, Union

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

import beanie
from beanie.exceptions import MongoDBVersionError
from beanie.odm.documents import Document
from beanie.odm.fields import ExpressionField, IndexModel, IndexModelFactory
from beanie.odm.union_doc import UnionDoc
from beanie.odm.views import View

DocumentLike = Union[Document, View, UnionDoc]


async def init_timeseries(
    cls: Type[Document], database: AsyncIOMotorDatabase
) -> None:
    settings = cls.get_settings()
    if settings.timeseries:
        if beanie.DATABASE_MAJOR_VERSION < 5:
            raise MongoDBVersionError(
                "Timeseries are supported by MongoDB version 5 and higher"
            )
        collection_names = await database.list_collection_names(
            authorizedCollections=True, nameOnly=True
        )
        if settings.name not in collection_names:
            kwargs = settings.timeseries.to_dict()
            await database.create_collection(settings.name, **kwargs)


async def init_indexes(cls: Type[Document], drop_old: bool) -> None:
    new_indexes = []
    for k, v in cls.model_fields.items():
        try:
            f = next(m for m in v.metadata if isinstance(m, IndexModelFactory))
            new_indexes.append(f(v.alias or k))
        except StopIteration:
            pass

    settings = cls.get_settings()
    merge_indexes = IndexModel.merge_indexes
    if settings.merge_indexes:
        super_indexes: List[IndexModel] = []
        for superclass in reversed(cls.mro()):
            if issubclass(superclass, Document) and superclass != Document:
                if indexes := superclass.get_settings().indexes:
                    super_indexes = merge_indexes(super_indexes, indexes)
        new_indexes = merge_indexes(new_indexes, super_indexes)
    elif settings.indexes:
        new_indexes = merge_indexes(new_indexes, settings.indexes)

    # Only drop indexes if the user specifically allows for it
    collection = cls.get_motor_collection()
    if drop_old:
        index_info = await collection.index_information()
        for index in IndexModel.iter_indexes(index_info):
            if index not in new_indexes:
                await collection.drop_index(index.name)

    # create indices
    if new_indexes:
        await collection.create_indexes(new_indexes)


async def init_view(
    cls: Type[View], database: AsyncIOMotorDatabase, recreate: bool
) -> None:
    settings = cls.get_settings()
    collection_names = await database.list_collection_names(
        authorizedCollections=True, nameOnly=True
    )
    if recreate or settings.name not in collection_names:
        if settings.name in collection_names:
            await settings.motor_collection.drop()
        await database.command(
            {
                "create": settings.name,
                "viewOn": settings.source,
                "pipeline": settings.pipeline,
            }
        )


def resolve_name(name: str) -> Type[DocumentLike]:
    try:
        module_name, class_name = name.rsplit(".", 1)
    except ValueError:
        raise ValueError(
            f"'{name}' doesn't have '.' path, eg. path.to.model.class"
        )
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    if not isinstance(cls, type):
        raise TypeError(f"'{name}' is not a class")
    return cls


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
    document_models: Optional[Sequence[Union[Type[DocumentLike], str]]] = None,
    allow_index_dropping: bool = False,
    recreate_views: bool = False,
    multiprocessing_mode: bool = False,
) -> None:
    """
    Beanie initialization

    :param database: AsyncIOMotorDatabase - motor database instance
    :param connection_string: str - MongoDB connection string
    :param document_models: List[Union[Type[DocumentLike], str]] - model classes
    or strings with dot separated paths
    :param allow_index_dropping: bool - if index dropping is allowed. Default False
    :param recreate_views: bool - if views should be recreated. Default False
    :param multiprocessing_mode: bool - if multiprocessing mode is on
        it will patch the motor client to use process's event loop. Default False
    :return: None
    """
    if document_models is None:
        raise ValueError("document_models parameter must be set")

    if connection_string is None and database is None:
        raise ValueError("Either connection_string or database must be set")

    if connection_string is not None and database is not None:
        raise ValueError("Either connection_string or database must be set")

    if database is None:
        client = AsyncIOMotorClient(connection_string)
        database = client.get_default_database()

    if multiprocessing_mode:
        database.client.get_io_loop = asyncio.get_running_loop  # type: ignore[method-assign]

    build_info = await database.command({"buildInfo": 1})
    beanie.DATABASE_MAJOR_VERSION = int(build_info["version"].split(".")[0])

    models: List[Type[DocumentLike]] = []
    for model in document_models:
        if isinstance(model, str):
            model = resolve_name(model)
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
        if issubclass(model, BaseModel):
            # inject an ExpressionField for each model field
            # this cannot live in __pydantic_init_subclass__ because in case of inheritance
            # Pydantic raises "Field name "{k}" shadows an attribute in parent ..."
            for k, v in model.model_fields.items():
                setattr(model, k, ExpressionField(v.alias or k))

        model.init_from_database(database)
        if issubclass(model, Document):
            await init_timeseries(model, database)
            await init_indexes(model, allow_index_dropping)
        if issubclass(model, View):
            await init_view(model, database, recreate_views)
        if hasattr(model, "custom_init"):
            await model.custom_init()
