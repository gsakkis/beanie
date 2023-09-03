import importlib
import inspect
from dataclasses import dataclass, field
from operator import attrgetter
from typing import List, Optional, Set, Type, Union, get_args, get_origin

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pymongo import IndexModel

from beanie.exceptions import MongoDBVersionError
from beanie.odm.actions import ActionRegistry
from beanie.odm.cache import LRUCache
from beanie.odm.documents import Document
from beanie.odm.fields import (
    DOCS_REGISTRY,
    BackLink,
    ExpressionField,
    Link,
    LinkInfo,
    LinkTypes,
)
from beanie.odm.settings.document import DocumentSettings, IndexModelField
from beanie.odm.settings.union_doc import UnionDocSettings
from beanie.odm.settings.view import ViewSettings
from beanie.odm.union_doc import UnionDoc
from beanie.odm.views import View

DocumentLike = Union[Document, View, UnionDoc]


@dataclass
class Output:
    class_name: str
    collection_name: str

    @classmethod
    def from_doctype(cls, doc_type: Type[Document]) -> "Output":
        class_name = doc_type._class_id
        collection_name = doc_type.get_collection_name()
        assert class_name is not None and collection_name is not None
        return cls(class_name, collection_name)


@dataclass(frozen=True)
class Initializer:
    database: AsyncIOMotorDatabase
    allow_index_dropping: bool
    recreate_views: bool
    inited_classes: List[Type[Document]] = field(default_factory=list)

    # Document

    @staticmethod
    def init_cache(cls) -> None:
        """
        Init model's cache
        :return: None
        """
        settings = cls.get_settings()
        if settings.use_cache:
            cls._cache = LRUCache(
                capacity=settings.cache_capacity,
                expiration_time=settings.cache_expiration_time,
            )

    @staticmethod
    def init_actions(cls):
        """
        Init event-based actions
        """
        ActionRegistry.clean_actions(cls)
        for attr in dir(cls):
            f = getattr(cls, attr)
            if inspect.isfunction(f) and hasattr(f, "has_action"):
                ActionRegistry.add_action(
                    document_class=cls,
                    event_types=f.event_types,
                    action_direction=f.action_direction,
                    funct=f,
                )

    async def init_document_collection(self, cls):
        """
        Init collection for the Document-based class
        :param cls:
        :return:
        """
        settings = cls.get_settings()

        # register in the Union Doc
        union_doc = settings.union_doc
        if union_doc is not None:
            name = settings.name or cls.__name__
            settings.name = union_doc.register_doc(name, cls)
            settings.union_doc_alias = name

        # set a name
        if not settings.name:
            settings.name = cls.__name__

        collection = self.database[settings.name]
        timeseries = settings.timeseries
        if timeseries is not None:
            if cls._database_major_version < 5:
                raise MongoDBVersionError(
                    "Timeseries are supported by MongoDB version 5 and higher"
                )
            collections = await self.database.list_collection_names()
            if settings.name not in collections:
                collection = await self.database.create_collection(
                    **timeseries.build_query(settings.name)
                )
        settings.motor_collection = collection

    async def init_indexes(self, cls, allow_index_dropping: bool = False):
        """
        Async indexes initializer
        """
        collection = cls.get_motor_collection()
        settings = cls.get_settings()

        index_information = await collection.index_information()

        old_indexes = IndexModelField.from_motor_index_information(
            index_information
        )
        new_indexes = []

        # Indexed field wrapped with Indexed()
        found_indexes = [
            IndexModelField(
                IndexModel(
                    [
                        (
                            fvalue.alias or k,
                            fvalue.annotation._indexed[0],
                        )
                    ],
                    **fvalue.annotation._indexed[1],
                )
            )
            for k, fvalue in cls.model_fields.items()
            if getattr(fvalue.annotation, "_indexed", False)
        ]

        if settings.merge_indexes:
            result: List[IndexModelField] = []
            for subclass in reversed(cls.mro()):
                if issubclass(subclass, Document) and not subclass == Document:
                    if (
                        subclass not in self.inited_classes
                        and not subclass == cls
                    ):
                        await self.init_class(subclass)
                    if indexes := subclass.get_settings().indexes:
                        result = IndexModelField.merge_indexes(result, indexes)
            found_indexes = IndexModelField.merge_indexes(
                found_indexes, result
            )

        else:
            if settings.indexes:
                found_indexes = IndexModelField.merge_indexes(
                    found_indexes, settings.indexes
                )

        new_indexes += found_indexes

        # delete indexes
        # Only drop indexes if the user specifically allows for it
        if allow_index_dropping:
            for index in IndexModelField.list_difference(
                old_indexes, new_indexes
            ):
                await collection.drop_index(index.name)

        # create indices
        if found_indexes:
            new_indexes += await collection.create_indexes(
                IndexModelField.list_to_index_model(new_indexes)
            )

    async def init_document(self, cls: Type[Document]) -> Optional[Output]:
        """
        Init Document-based class

        :param cls:
        :return:
        """
        if cls is Document:
            return None

        # get db version
        build_info = await self.database.command({"buildInfo": 1})
        mongo_version = build_info["version"]
        cls._database_major_version = int(mongo_version.split(".")[0])
        if cls not in self.inited_classes:
            settings = DocumentSettings.model_validate(
                cls.Settings.__dict__ if hasattr(cls, "Settings") else {}
            )
            cls._document_settings = settings
            cls._children = dict()
            cls._parent = None
            cls._inheritance_inited = False
            cls._class_id = None
            cls._link_fields = None

            bases = [b for b in cls.__bases__ if issubclass(b, Document)]
            if len(bases) > 1:
                return None

            parent = bases[0]
            output = await self.init_document(parent)
            if settings.is_root and (
                parent is Document or not parent.get_settings().is_root
            ):
                if settings.name is None:
                    settings.name = cls.__name__
                cls._class_id = cls.__name__
                output = Output.from_doctype(cls)
                cls._inheritance_inited = True
            elif output is not None:
                class_id = f"{output.class_name}.{cls.__name__}"
                cls._class_id = class_id
                output.class_name = class_id
                settings.name = output.collection_name
                cls._parent = parent
                while parent is not None:
                    parent._children[class_id] = cls
                    parent = parent._parent
                cls._inheritance_inited = True

            await self.init_document_collection(cls)
            await self.init_indexes(cls, self.allow_index_dropping)
            self.init_fields(cls)
            cls.set_hidden_fields()
            self.init_cache(cls)
            self.init_actions(cls)

            self.inited_classes.append(cls)

            return output

        elif cls._inheritance_inited:
            return Output.from_doctype(cls)
        else:
            return None

    # Views

    def init_fields(self, cls) -> None:
        if cls._link_fields is None:
            cls._link_fields = {}
        for k, v in cls.model_fields.items():
            setattr(cls, k, ExpressionField(v.alias or k))
            link_info = detect_link(v, k)
            if link_info is not None:
                cls._link_fields[k] = link_info
                check_nested_links(link_info)

    async def init_view(self, cls: Type[View]):
        """
        Init View-based class

        :param cls:
        :return:
        """
        settings = ViewSettings.model_validate(
            cls.Settings.__dict__ if hasattr(cls, "Settings") else {}
        )
        if settings.name is None:
            settings.name = cls.__name__
        if inspect.isclass(settings.source):
            settings.source = settings.source.get_collection_name()
        settings.motor_collection = self.database[settings.name]
        cls._settings = settings

        self.init_fields(cls)
        self.init_cache(cls)

        collection_names = await self.database.list_collection_names()
        if self.recreate_views or settings.name not in collection_names:
            if settings.name in collection_names:
                await cls.get_motor_collection().drop()
            await self.database.command(
                {
                    "create": settings.name,
                    "viewOn": settings.source,
                    "pipeline": settings.pipeline,
                }
            )

    # Union Doc

    async def init_union_doc(self, cls: Type[UnionDoc]):
        """
        Init Union Doc based class

        :param cls:
        :return:
        """
        settings = UnionDocSettings.model_validate(
            cls.Settings.__dict__ if hasattr(cls, "Settings") else {}
        )
        if settings.name is None:
            settings.name = cls.__name__
        settings.motor_collection = self.database[settings.name]
        cls._settings = settings
        cls._is_inited = True

    # Final

    async def init_class(self, cls: Type[DocumentLike]):
        """
        Init Document, View or UnionDoc based class.

        :param cls:
        :return:
        """
        if issubclass(cls, Document):
            await self.init_document(cls)
        if issubclass(cls, View):
            await self.init_view(cls)
        if issubclass(cls, UnionDoc):
            await self.init_union_doc(cls)
        if hasattr(cls, "custom_init"):
            await cls.custom_init()


def detect_link(field_info: FieldInfo, field_name: str) -> Optional[LinkInfo]:
    """It detects link and returns LinkInfo if any found"""
    annotation = field_info.annotation
    origin = get_origin(annotation)
    args = get_args(annotation)
    for cls in Link, BackLink:
        if cls is Link:
            lookup_field_name = field_name
        elif field_info.json_schema_extra is not None:
            lookup_field_name = field_info.json_schema_extra.get(  # type: ignore
                "original_field"
            )

        # Check if annotation is one of the custom classes
        if origin is cls:
            link_type = LinkTypes["DIRECT" if cls is Link else "BACK_DIRECT"]
            return LinkInfo(
                field_name=field_name,
                lookup_field_name=lookup_field_name,
                document_class=DOCS_REGISTRY.evaluate_fr(args[0]),
                link_type=link_type,
            )

        # Check if annotation is List[custom class]
        if (
            origin in (List, list)
            and getattr(args[0], "__origin__", None) is cls
        ):
            link_type = LinkTypes["LIST" if cls is Link else "BACK_LIST"]
            return LinkInfo(
                field_name=field_name,
                lookup_field_name=lookup_field_name,
                document_class=DOCS_REGISTRY.evaluate_fr(get_args(args[0])[0]),
                link_type=link_type,
            )

        # Check if annotation is Optional[custom class] or Optional[List[custom class]]
        if not (origin is Union and len(args) == 2 and args[1] is type(None)):
            continue

        optional_origin = get_origin(args[0])
        optional_args = get_args(args[0])
        if optional_origin is cls:
            link_type = LinkTypes[
                "OPTIONAL_DIRECT" if cls is Link else "OPTIONAL_BACK_DIRECT"
            ]
            return LinkInfo(
                field_name=field_name,
                lookup_field_name=lookup_field_name,
                document_class=DOCS_REGISTRY.evaluate_fr(optional_args[0]),
                link_type=link_type,
            )

        if (
            optional_origin in (List, list)
            and getattr(optional_args[0], "__origin__", None) is cls
        ):
            link_type = LinkTypes[
                "OPTIONAL_LIST" if cls is Link else "OPTIONAL_BACK_LIST"
            ]
            return LinkInfo(
                field_name=field_name,
                lookup_field_name=lookup_field_name,
                document_class=DOCS_REGISTRY.evaluate_fr(
                    get_args(optional_args[0])[0]
                ),
                link_type=link_type,
            )

    return None


def check_nested_links(
    link_info: LinkInfo, checked: Optional[Set[Type[BaseModel]]] = None
):
    if checked is None:
        checked = set()
    document_class = link_info.document_class
    if document_class in checked:
        return
    checked.add(document_class)
    for k, v in document_class.model_fields.items():
        nested_link_info = detect_link(v, k)
        if nested_link_info is None:
            continue
        if link_info.nested_links is None:
            link_info.nested_links = {}
        link_info.nested_links[k] = nested_link_info
        check_nested_links(nested_link_info, checked)


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
    database: AsyncIOMotorDatabase = None,
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
    :param allow_index_dropping: bool - if index dropping is allowed.
    Default False
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

    initializer = Initializer(
        database=database,
        allow_index_dropping=allow_index_dropping,
        recreate_views=recreate_views,
    )
    models = list(map(register_document_model, document_models))
    models.sort(key=attrgetter("_sort_order"))
    for model in models:
        await initializer.init_class(model)
