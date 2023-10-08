from enum import Enum
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    ClassVar,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    Union,
)
from uuid import UUID, uuid4

import pymongo
from bson import DBRef
from lazy_model import LazyModel
from motor.motor_asyncio import AsyncIOMotorDatabase
from pydantic import ConfigDict, Field, TypeAdapter, ValidationError
from pymongo.client_session import ClientSession
from pymongo.errors import DuplicateKeyError
from pymongo.results import DeleteResult, InsertManyResult
from typing_extensions import Annotated, Literal, Self

from beanie.exceptions import (
    DocumentNotFound,
    DocumentWasNotSaved,
    NotSupported,
    ReplaceError,
    RevisionIdWasChanged,
)
from beanie.odm.actions import ActionDirections, ActionRegistry, EventTypes
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.fields import IndexModel, PydanticObjectId
from beanie.odm.interfaces.find import FindInterface
from beanie.odm.interfaces.settings import BaseSettings, SettingsInterface
from beanie.odm.links import Link, LinkedModelMixin, LinkInfo
from beanie.odm.models import (
    InspectionError,
    InspectionResult,
    InspectionStatuses,
)
from beanie.odm.operators import BaseOperator
from beanie.odm.operators import update as update_ops
from beanie.odm.operators.comparison import In
from beanie.odm.queries import FieldNameMapping
from beanie.odm.queries.update import UpdateMany, UpdateResponse
from beanie.odm.state import (
    BaseDocumentState,
    DocumentState,
    PreviousDocumentState,
)
from beanie.odm.timeseries import TimeSeriesConfig
from beanie.odm.union_doc import UnionDoc
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import merge_models
from beanie.odm.utils.typing import extract_id_class

if TYPE_CHECKING:
    from pydantic.main import IncEx


class DocumentSettings(BaseSettings):
    use_state_management: bool = False
    state_management_replace_objects: bool = False
    state_management_save_previous: bool = False
    validate_on_save: bool = False
    use_revision: bool = False

    indexes: List[IndexModel] = Field(default_factory=list)
    merge_indexes: bool = False
    timeseries: Optional[TimeSeriesConfig] = None

    union_doc: Optional[Type[UnionDoc]] = None
    union_doc_alias: Optional[str] = None

    keep_nulls: bool = True
    is_root: bool = False


class DeleteRules(str, Enum):
    DO_NOTHING = "DO_NOTHING"
    DELETE_LINKS = "DELETE_LINKS"


class WriteRules(str, Enum):
    DO_NOTHING = "DO_NOTHING"
    WRITE = "WRITE"


def _json_schema_extra(schema: Dict[str, Any]) -> None:
    props = {}
    for k, v in schema.get("properties", {}).items():
        if not v.get("hidden", False):
            props[k] = v
    schema["properties"] = props


class Document(
    LazyModel,
    LinkedModelMixin,
    SettingsInterface[DocumentSettings],
    FindInterface,
):
    """
    Document Mapping class.

    Fields:

    - `id` - MongoDB document ObjectID "_id" field.
    Mapped to the PydanticObjectId class
    """

    model_config = ConfigDict(
        json_schema_extra=_json_schema_extra,
        populate_by_name=True,
        alias_generator=lambda s: "_id" if s == "id" else s,
    )

    id: Optional[PydanticObjectId] = Field(
        default=None, description="MongoDB document ObjectID"
    )

    # State
    revision_id: Optional[UUID] = Field(
        default=None, json_schema_extra={"hidden": True}
    )
    _previous_revision_id: Optional[UUID] = None
    __state: BaseDocumentState

    # Inheritance
    _class_id: ClassVar[Optional[str]] = None
    _children: ClassVar[Dict[str, Type[Self]]]

    # Other
    _settings_type = DocumentSettings
    _id_class: ClassVar[type]
    _id_adapter: ClassVar[TypeAdapter[Any]]
    _hidden_fields: ClassVar[Set[str]] = set()

    @classmethod
    def init_from_database(cls, database: AsyncIOMotorDatabase) -> None:
        cls.set_settings(database)
        settings = cls.get_settings()

        # register in the UnionDoc
        if union_doc := settings.union_doc:
            union_doc._children[settings.name] = cls
            settings.union_doc_alias = settings.name
            settings.name = union_doc.get_collection_name()

        # set up document inheritance
        cls._children = {}
        parent_cls = cls._parent_document_cls()
        if settings.is_root and not (
            parent_cls and parent_cls.get_settings().is_root
        ):
            cls._class_id = cls.__name__
        elif parent_cls and parent_cls._class_id:
            # set the common collection name if this document class is part of an
            # inheritance chain with is_root = True
            settings.name = parent_cls.get_collection_name()
            cls._class_id = class_id = f"{parent_cls._class_id}.{cls.__name__}"
            while parent_cls is not None:
                parent_cls._children[class_id] = cls
                parent_cls = parent_cls._parent_document_cls()

        # set up id class and adapter
        id_field = cls.model_fields["id"]
        id_annotation = id_field.annotation
        cls._id_class = extract_id_class(id_annotation)
        if id_field.metadata:
            id_annotation = Annotated[(id_annotation, *id_field.metadata)]
        cls._id_adapter = TypeAdapter(id_annotation)

        # set up hidden fields
        cls._hidden_fields = set()
        for k, v in cls.model_fields.items():
            if isinstance(
                v.json_schema_extra, dict
            ) and v.json_schema_extra.get("hidden"):
                cls._hidden_fields.add(k)

        # set up actions
        ActionRegistry.register_type(cls)

    @classmethod
    def lazy_parse(cls, data: Mapping[str, Any]) -> Self:
        self: Document = super().lazy_parse(data, fields={"_id"})
        self._state.saved = {"_id": self.id}
        return self

    @classmethod
    async def get(
        cls,
        document_id: Any,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        with_children: bool = False,
        **pymongo_kwargs: Any,
    ) -> Optional[Self]:
        """
        Get document by id, returns None if document does not exist

        :param document_id: PydanticObjectId - document id
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool - ignore cache (if it is turned on)
        :param **pymongo_kwargs: pymongo native parameters for find operation
        :return: Union["Document", None]
        """
        return await cls.find_one(
            {"_id": cls._parse_document_id(document_id)},
            session=session,
            ignore_cache=ignore_cache,
            fetch_links=fetch_links,
            with_children=with_children,
            **pymongo_kwargs,
        )

    @ActionRegistry.wrap_with_actions(EventTypes.INSERT)
    async def insert(
        self,
        *,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        session: Optional[ClientSession] = None,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> Self:
        """
        Insert the document (self) to the collection
        :return: Document
        """
        await self._validate_self(skip_actions=skip_actions)
        if self.get_settings().use_revision:
            self.revision_id = uuid4()
        if link_rule == WriteRules.WRITE:
            for field_info in self.get_link_fields().values():
                if not field_info.link_type.is_back:
                    for subdoc in self._iter_linked_documents(field_info):
                        await subdoc.save(
                            link_rule=WriteRules.WRITE, session=session
                        )
        result = await self.get_motor_collection().insert_one(
            self.get_dict(), session=session
        )
        self.id = self._parse_document_id(result.inserted_id)
        self._swap_revision()
        self._save_state()
        return self

    async def create(self, session: Optional[ClientSession] = None) -> Self:
        """
        The same as self.insert()
        :return: Document
        """
        return await self.insert(session=session)

    @classmethod
    async def insert_one(
        cls,
        document: Self,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional["BulkWriter"] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
    ) -> Optional[Self]:
        """
        Insert one document to the collection
        :param document: Document - document to insert
        :param session: ClientSession - pymongo session
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param link_rule: InsertRules - hot to manage link fields
        :return: Document
        """
        if not isinstance(document, cls):
            raise TypeError(
                "Inserting document must be of the original document class"
            )
        if bulk_writer is None:
            return await document.insert(link_rule=link_rule, session=session)
        else:
            if link_rule == WriteRules.WRITE:
                raise NotSupported(
                    "Cascade insert with bulk writing not supported"
                )
            bulk_writer.add_operation(
                Operation(
                    operation_class=pymongo.InsertOne,
                    first_query=document.get_dict(),
                    object_class=type(document),
                )
            )
            return None

    @classmethod
    async def insert_many(
        cls,
        documents: Iterable[Self],
        session: Optional[ClientSession] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        **pymongo_kwargs: Any,
    ) -> InsertManyResult:
        """
        Insert many documents to the collection

        :param documents:  List["Document"] - documents to insert
        :param session: ClientSession - pymongo session
        :param link_rule: InsertRules - how to manage link fields
        :return: InsertManyResult
        """
        if link_rule == WriteRules.WRITE:
            raise NotSupported(
                "Cascade insert not supported for insert many method"
            )
        documents_list = [doc.get_dict() for doc in documents]
        return await cls.get_motor_collection().insert_many(
            documents_list, session=session, **pymongo_kwargs
        )

    @ActionRegistry.wrap_with_actions(EventTypes.REPLACE)
    async def replace(
        self,
        *,
        ignore_revision: bool = False,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> Self:
        """
        Fully update the document in the database

        :param session: Optional[ClientSession] - pymongo session.
        :param ignore_revision: bool - do force replace.
            Used when revision based protection is turned on.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :return: self
        """
        await self._validate_self(skip_actions=skip_actions)
        if self.id is None:
            raise ValueError("Document must have an id")

        if bulk_writer is not None and link_rule != WriteRules.DO_NOTHING:
            raise NotSupported

        if link_rule == WriteRules.WRITE:
            for field_info in self.get_link_fields().values():
                for subdoc in self._iter_linked_documents(field_info):
                    await subdoc.replace(
                        link_rule=link_rule,
                        bulk_writer=bulk_writer,
                        ignore_revision=ignore_revision,
                        session=session,
                    )

        use_revision_id = self.get_settings().use_revision
        find_query: Dict[str, Any] = {"_id": self.id}
        if use_revision_id and not ignore_revision:
            find_query["revision_id"] = self._previous_revision_id
        try:
            await self.find_one(find_query).replace(
                self,
                session=session,
                bulk_writer=bulk_writer,
            )
        except DocumentNotFound:
            if use_revision_id and not ignore_revision:
                raise RevisionIdWasChanged
            else:
                raise DocumentNotFound
        self._swap_revision()
        self._save_state()
        return self

    @classmethod
    async def replace_many(
        cls, documents: List[Self], session: Optional[ClientSession] = None
    ) -> None:
        """
        Replace list of documents

        :param documents: List["Document"]
        :param session: Optional[ClientSession] - pymongo session.
        :return: None
        """
        ids_list = [document.id for document in documents]
        if await cls.find_many(In("_id", ids_list)).count() != len(ids_list):
            raise ReplaceError(
                "Some of the documents are not exist in the collection"
            )
        async with BulkWriter(session=session) as bulk_writer:
            for document in documents:
                await document.replace(
                    bulk_writer=bulk_writer, session=session
                )

    @ActionRegistry.wrap_with_actions(EventTypes.SAVE)
    async def save(
        self,
        *,
        session: Optional[ClientSession] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        ignore_revision: bool = False,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Update an existing model in the database or
        insert it if it does not yet exist.

        :param session: Optional[ClientSession] - pymongo session.
        :param link_rule: WriteRules - rules how to deal with links on writing
        :param ignore_revision: bool - do force save.
        :return: self
        """
        await self._validate_self(skip_actions=skip_actions)
        if link_rule == WriteRules.WRITE:
            for field_info in self.get_link_fields().values():
                for subdoc in self._iter_linked_documents(field_info):
                    await subdoc.save(link_rule=link_rule, session=session)

        update_args: List[BaseOperator] = [update_ops.Set(self.get_dict())]
        if self.get_settings().keep_nulls is False:
            update_args.append(update_ops.Unset(self._get_top_level_nones()))
        result = await self.update(
            *update_args,
            ignore_revision=ignore_revision,
            session=session,
            skip_actions=skip_actions,
            upsert=True,
            **pymongo_kwargs,
        )
        self._save_state()
        return result

    @ActionRegistry.wrap_with_actions(EventTypes.SAVE_CHANGES)
    async def save_changes(
        self,
        *,
        ignore_revision: bool = False,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> Self:
        """
        Save changes.
        State management usage must be turned on

        :param ignore_revision: bool - ignore revision id, if revision is turned on
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :return: self
        """
        await self._validate_self(skip_actions=skip_actions)
        if not self.is_changed:
            return self
        update_args: List[BaseOperator] = [update_ops.Set(self.get_changes())]
        if self.get_settings().keep_nulls is False:
            update_args.append(update_ops.Unset(self._get_top_level_nones()))
        return await self.update(
            *update_args,
            ignore_revision=ignore_revision,
            session=session,
            bulk_writer=bulk_writer,
            skip_actions=skip_actions,
        )

    @ActionRegistry.wrap_with_actions(EventTypes.UPDATE)
    async def update(
        self,
        *args: FieldNameMapping,
        ignore_revision: bool = False,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Partially update the document in the database

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: ClientSession - pymongo session.
        :param ignore_revision: bool - force update. Will update even if revision id is not the same, as stored
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: None
        """
        arguments = list(args)
        use_revision_id = self.get_settings().use_revision

        find_query: Dict[str, Any] = {
            "_id": self.id if self.id is not None else PydanticObjectId()
        }
        if use_revision_id and not ignore_revision:
            find_query["revision_id"] = self._previous_revision_id

        if use_revision_id:
            arguments.append(update_ops.SetRevisionId(self.revision_id))
        try:
            result = await self.find_one(find_query).update(
                *arguments,
                session=session,
                response_type=UpdateResponse.NEW_DOCUMENT,
                bulk_writer=bulk_writer,
                **pymongo_kwargs,
            )
        except DuplicateKeyError:
            raise RevisionIdWasChanged
        if bulk_writer is None:
            if use_revision_id and not ignore_revision and result is None:
                raise RevisionIdWasChanged
            if result is not None:
                assert isinstance(result, Document)
                merge_models(self, result)
        self._save_state()
        return self

    @classmethod
    def update_all(
        cls,
        *args: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateMany:
        """
        Partially update all the documents

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: ClientSession - pymongo session.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param **pymongo_kwargs: pymongo native parameters for find operation
        :return: UpdateMany query
        """
        return cls.find_all().update(
            *args, session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    async def set(
        self,
        expression: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Set values

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).set({Sample.one: 100})

        ```

        Uses [Set operator](operators/update.md#set)

        :param expression: Dict[Union[ExpressionField, str], Any] - keys and
        values to set
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return await self.update(
            update_ops.Set(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    async def current_date(
        self,
        expression: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Set current date

        Uses [CurrentDate operator](operators/update.md#currentdate)

        :param expression: Dict[Union[ExpressionField, str], Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return await self.update(
            update_ops.CurrentDate(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    async def inc(
        self,
        expression: FieldNameMapping,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Increment

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).inc({Sample.one: 100})

        ```

        Uses [Inc operator](operators/update.md#inc)

        :param expression: Dict[Union[ExpressionField, str], Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return await self.update(
            update_ops.Inc(expression),
            session=session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    @ActionRegistry.wrap_with_actions(EventTypes.DELETE)
    async def delete(
        self,
        *,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        link_rule: DeleteRules = DeleteRules.DO_NOTHING,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
        **pymongo_kwargs: Any,
    ) -> Optional[DeleteResult]:
        """
        Delete the document

        :param session: Optional[ClientSession] - pymongo session.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param link_rule: DeleteRules - rules for link fields
        :param **pymongo_kwargs: pymongo native parameters for delete operation
        :return: Optional[DeleteResult] - pymongo DeleteResult instance.
        """
        if link_rule == DeleteRules.DELETE_LINKS:
            for field_info in self.get_link_fields().values():
                for subdoc in self._iter_linked_documents(field_info):
                    await subdoc.delete(
                        link_rule=DeleteRules.DELETE_LINKS,
                        session=session,
                        **pymongo_kwargs,
                    )
        return await self.find_one({"_id": self.id}).delete(
            session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    @classmethod
    async def delete_all(
        cls,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Optional[DeleteResult]:
        """
        Delete all the documents

        :param session: Optional[ClientSession] - pymongo session.
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param **pymongo_kwargs: pymongo native parameters for delete operation
        :return: Optional[DeleteResult] - pymongo DeleteResult instance.
        """
        return await cls.find_all().delete(
            session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    # State management

    @property
    def is_changed(self) -> bool:
        return self._state.is_changed

    @property
    def has_changed(self) -> bool:
        return self._state.has_changed

    def get_changes(self) -> Dict[str, Any]:
        return self._state.get_changes()

    def get_previous_changes(self) -> Dict[str, Any]:
        return self._state.get_previous_changes()

    def rollback(self) -> None:
        if self.is_changed:
            saved = self._state.saved
            assert saved is not None
            for key, value in saved.items():
                setattr(self, key if key != "_id" else "id", value)

    # Other

    @classmethod
    async def distinct(
        cls,
        key: str,
        filter: Optional[Mapping[str, Any]] = None,
        session: Optional[ClientSession] = None,
        **pymongo_kwargs: Any,
    ) -> List[Any]:
        return await cls.get_motor_collection().distinct(
            key, filter, session, **pymongo_kwargs
        )

    @classmethod
    async def inspect_collection(
        cls, session: Optional[ClientSession] = None
    ) -> InspectionResult:
        """
        Check, if documents, stored in the MongoDB collection
        are compatible with the Document schema

        :return: InspectionResult
        """
        inspection_result = InspectionResult()
        async for json_document in cls.get_motor_collection().find(
            {}, session=session
        ):
            try:
                cls.model_validate(json_document)
            except ValidationError as e:
                if inspection_result.status == InspectionStatuses.OK:
                    inspection_result.status = InspectionStatuses.FAIL
                inspection_result.errors.append(
                    InspectionError(
                        document_id=json_document["_id"], error=str(e)
                    )
                )
        return inspection_result

    def model_dump(
        self,
        *,
        mode: Literal["json", "python"] = "python",
        include: "IncEx" = None,
        exclude: "IncEx" = None,
        by_alias: bool = False,
        exclude_hidden: bool = True,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
        round_trip: bool = False,
        warnings: bool = True,
    ) -> Dict[str, Any]:
        """
        Overriding of the respective method from Pydantic
        Hides fields, marked as "hidden
        """
        if exclude_hidden:
            if isinstance(exclude, AbstractSet):
                exclude = {*self._hidden_fields, *exclude}
            elif isinstance(exclude, Mapping):
                exclude = dict(
                    {k: True for k in self._hidden_fields}, **exclude
                )
            elif exclude is None:
                exclude = self._hidden_fields

        kwargs = {
            "mode": mode,
            "include": include,
            "exclude": exclude,
            "by_alias": by_alias,
            "exclude_unset": exclude_unset,
            "exclude_defaults": exclude_defaults,
            "exclude_none": exclude_none,
            "round_trip": round_trip,
            "warnings": warnings,
        }
        dump: Dict[str, Any] = super().model_dump(**kwargs)
        return dump

    def get_dict(
        self,
        to_db: bool = True,
        exclude: Optional[Set[str]] = None,
        keep_nulls: Optional[bool] = None,
    ) -> Dict[str, Any]:
        settings = self.get_settings()
        if exclude is None:
            exclude = set()
        if self.id is None:
            exclude.add("_id")
        if not settings.use_revision:
            exclude.add("revision_id")
        if keep_nulls is None:
            keep_nulls = settings.keep_nulls
        encoder = Encoder(exclude=exclude, to_db=to_db, keep_nulls=keep_nulls)
        data: Dict[str, Any] = encoder.encode(self)
        return data

    def to_ref(self) -> DBRef:
        if self.id is None:
            raise DocumentWasNotSaved("Can not create dbref without id")
        return DBRef(self.get_collection_name(), self.id)

    @classmethod
    def link_from_id(cls, id: Any) -> Link[Self]:
        return Link(DBRef(id=id, collection=cls.get_collection_name()), cls)

    # internal

    @classmethod
    def _parent_document_cls(cls) -> Optional[Type[Self]]:
        parent_cls = next(b for b in cls.__bases__ if issubclass(b, Document))
        return parent_cls if parent_cls is not Document else None

    @classmethod
    def _parse_document_id(cls, document_id: Any) -> Any:
        if isinstance(document_id, cls._id_class):
            return document_id
        return cls._id_adapter.validate_python(document_id)

    @classmethod
    def _get_class_id_filter(
        cls, with_children: bool = False
    ) -> Optional[Any]:
        if cls._class_id:
            if with_children:
                return {"$in": [cls._class_id, *cls._children.keys()]}
            else:
                return cls._class_id

        settings = cls.get_settings()
        return settings.union_doc_alias if settings.union_doc else None

    def _iter_linked_documents(self, link_info: LinkInfo) -> Iterable[Self]:
        objs = []
        value = getattr(self, link_info.field_name)
        if not link_info.link_type.is_list:
            objs.append(value)
        elif isinstance(value, list):
            objs.extend(value)
        return (obj for obj in objs if isinstance(obj, Document))

    def _get_top_level_nones(
        self, exclude: Optional[Set[str]] = None
    ) -> Dict[str, Any]:
        d = self.get_dict(exclude=exclude, to_db=False, keep_nulls=True)
        return {k: v for k, v in d.items() if v is None}

    @ActionRegistry.wrap_with_actions(EventTypes.VALIDATE_ON_SAVE)
    async def _validate_self(
        self,
        *,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> None:
        # TODO: it can be sync, but needs some actions controller improvements
        if self.get_settings().validate_on_save:
            data = self.model_dump()
            self.__class__.model_validate(data)

    @property
    def _state(self) -> BaseDocumentState:
        try:
            return self.__state
        except AttributeError:
            pass
        settings = self.get_settings()
        if not settings.use_state_management:
            state = BaseDocumentState()
        else:
            if not settings.state_management_save_previous:
                cls = DocumentState
            else:
                cls = PreviousDocumentState
            replace_objects = settings.state_management_replace_objects
            state = cls(self.get_dict, replace_objects)
        self.__state = state
        return state

    def _save_state(self) -> None:
        if self.id is not None:
            self._state.save()

    def _swap_revision(self) -> None:
        if self.get_settings().use_revision:
            self._previous_revision_id = self.revision_id
            self.revision_id = uuid4()
