import importlib
import inspect
from typing import List, Optional, Type, Union, cast

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel
from pymongo import IndexModel

import beanie
from beanie.odm.documents import Document
from beanie.odm.fields import ExpressionField, IndexModelField
from beanie.odm.links import DOCS_REGISTRY
from beanie.odm.union_doc import UnionDoc
from beanie.odm.views import View, ViewSettings

DocumentLike = Union[Document, View, UnionDoc]


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

    settings = cls.get_settings()
    merge_indexes = IndexModelField.merge_indexes
    if settings.merge_indexes:
        super_indexes: List[IndexModelField] = []
        for superclass in reversed(cls.mro()):
            if issubclass(superclass, Document) and superclass != Document:
                if indexes := superclass.get_settings().indexes:
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
        if issubclass(model, BaseModel):
            # inject an ExpressionField for each model field
            # this cannot live in __pydantic_init_subclass__ because in case of inheritance
            # Pydantic raises "Field name "{k}" shadows an attribute in parent ..."
            for k, v in model.model_fields.items():
                setattr(model, k, ExpressionField(v.alias or k))

        if issubclass(model, UnionDoc):
            await model.get_settings().update_from_database(database)
        elif issubclass(model, Document):
            await model.get_settings().update_from_database(database)
            await init_indexes(model, allow_index_dropping)
        elif issubclass(model, View):
            settings = cast(ViewSettings, model.get_settings())
            await settings.update_from_database(database, recreate_views)

        if hasattr(model, "custom_init"):
            await model.custom_init()
