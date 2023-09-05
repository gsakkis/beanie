import asyncio
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    ClassVar,
    Dict,
    List,
    Mapping,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
)
from uuid import UUID, uuid4

from bson import DBRef
from lazy_model import LazyModel
from pydantic import (
    ConfigDict,
    Field,
    PrivateAttr,
    TypeAdapter,
    ValidationError,
)
from pymongo import InsertOne
from pymongo.client_session import ClientSession
from pymongo.errors import DuplicateKeyError
from pymongo.results import DeleteResult, InsertManyResult
from typing_extensions import Self

from beanie.exceptions import (
    DocumentNotFound,
    DocumentWasNotSaved,
    NotSupported,
    ReplaceError,
    RevisionIdWasChanged,
)
from beanie.odm.actions import ActionDirections, EventTypes, wrap_with_actions
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.fields import (
    DeleteRules,
    ExpressionField,
    PydanticObjectId,
    WriteRules,
)
from beanie.odm.interfaces.find import FindInterface
from beanie.odm.links import Link, LinkedModel, LinkTypes
from beanie.odm.models import (
    InspectionError,
    InspectionResult,
    InspectionStatuses,
)
from beanie.odm.operators.find.comparison import In
from beanie.odm.operators.update.general import (
    CurrentDate,
    Inc,
    SetRevisionId,
    Unset,
)
from beanie.odm.operators.update.general import Set as SetOperator
from beanie.odm.queries.update import UpdateMany, UpdateResponse
from beanie.odm.settings.document import DocumentSettings
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import merge_models
from beanie.odm.utils.state import (
    previous_saved_state_needed,
    save_state_after,
    saved_state_needed,
    swap_revision_after,
)
from beanie.odm.utils.typing import extract_id_class

if TYPE_CHECKING:
    from pydantic.typing import AbstractSetIntStr, DictStrAny, MappingIntStrAny

DocType = TypeVar("DocType", bound="Document")


def json_schema_extra(schema: Dict[str, Any], model: Type["Document"]) -> None:
    props = {}
    for k, v in schema.get("properties", {}).items():
        if not v.get("hidden", False):
            props[k] = v
    schema["properties"] = props


class Document(LazyModel, LinkedModel, FindInterface):
    """
    Document Mapping class.

    Fields:

    - `id` - MongoDB document ObjectID "_id" field.
    Mapped to the PydanticObjectId class

    Inherited from:

    - Pydantic BaseModel
    - [UpdateMethods](https://roman-right.github.io/beanie/api/interfaces/#aggregatemethods)
    """

    model_config = ConfigDict(
        json_schema_extra=json_schema_extra,
        populate_by_name=True,
        alias_generator=lambda s: "_id" if s == "id" else s,
    )

    id: Optional[PydanticObjectId] = Field(
        default=None, description="MongoDB document ObjectID"
    )

    # Inheritance
    _class_id: ClassVar[Optional[str]] = None
    _children: ClassVar[Dict[str, Type["Document"]]]

    # State
    revision_id: Optional[UUID] = Field(default=None, hidden=True)
    _previous_revision_id: Optional[UUID] = PrivateAttr(default=None)
    _saved_state: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _previous_saved_state: Optional[Dict[str, Any]] = PrivateAttr(default=None)

    # Other
    _settings: ClassVar[DocumentSettings]
    _hidden_fields: ClassVar[Set[str]] = set()

    def swap_revision(self):
        if self._settings.use_revision:
            self._previous_revision_id = self.revision_id
            self.revision_id = uuid4()

    @classmethod
    def parent_document_cls(cls) -> Optional[Type["Document"]]:
        parent_cls = next(b for b in cls.__bases__ if issubclass(b, Document))
        return parent_cls if parent_cls is not Document else None

    @classmethod
    def _parse_document_id(cls, document_id: Any) -> Any:
        id_annotation = cls.model_fields["id"].annotation
        if isinstance(document_id, extract_id_class(id_annotation)):
            return document_id
        return TypeAdapter(id_annotation).validate_python(document_id)

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
        return cast(
            Optional[Self],
            await cls.find_one(
                {"_id": cls._parse_document_id(document_id)},
                session=session,
                ignore_cache=ignore_cache,
                fetch_links=fetch_links,
                with_children=with_children,
                **pymongo_kwargs,
            ),
        )

    @wrap_with_actions(EventTypes.INSERT)
    @save_state_after
    @swap_revision_after
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
        if self._settings.use_revision:
            self.revision_id = uuid4()
        if link_rule == WriteRules.WRITE:
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.save(link_rule=WriteRules.WRITE)
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                    ]:
                        if isinstance(value, List):
                            for obj in value:
                                if isinstance(obj, Document):
                                    await obj.save(link_rule=WriteRules.WRITE)
        result = await self.get_motor_collection().insert_one(
            self.get_dict(), session=session
        )
        self.id = self._parse_document_id(result.inserted_id)
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
        :return: DocType
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
                    operation=InsertOne,
                    first_query=document.get_dict(),
                    object_class=type(document),
                )
            )
            return None

    @classmethod
    async def insert_many(
        cls,
        documents: List[Self],
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

    @wrap_with_actions(EventTypes.REPLACE)
    @save_state_after
    @swap_revision_after
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
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                        LinkTypes.BACK_DIRECT,
                        LinkTypes.OPTIONAL_BACK_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.replace(
                                link_rule=link_rule,
                                bulk_writer=bulk_writer,
                                ignore_revision=ignore_revision,
                                session=session,
                            )
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                        LinkTypes.BACK_LIST,
                        LinkTypes.OPTIONAL_BACK_LIST,
                    ]:
                        if isinstance(value, List):
                            for obj in value:
                                if isinstance(obj, Document):
                                    await obj.replace(
                                        link_rule=link_rule,
                                        bulk_writer=bulk_writer,
                                        ignore_revision=ignore_revision,
                                        session=session,
                                    )

        use_revision_id = self._settings.use_revision
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
        return self

    @wrap_with_actions(EventTypes.SAVE)
    @save_state_after
    async def save(
        self,
        session: Optional[ClientSession] = None,
        link_rule: WriteRules = WriteRules.DO_NOTHING,
        ignore_revision: bool = False,
        **kwargs,
    ) -> None:
        """
        Update an existing model in the database or
        insert it if it does not yet exist.

        :param session: Optional[ClientSession] - pymongo session.
        :param link_rule: WriteRules - rules how to deal with links on writing
        :param ignore_revision: bool - do force save.
        :return: None
        """
        if link_rule == WriteRules.WRITE:
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                        LinkTypes.BACK_DIRECT,
                        LinkTypes.OPTIONAL_BACK_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.save(
                                link_rule=link_rule, session=session
                            )
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                        LinkTypes.BACK_LIST,
                        LinkTypes.OPTIONAL_BACK_LIST,
                    ]:
                        if isinstance(value, List):
                            for obj in value:
                                if isinstance(obj, Document):
                                    await obj.save(
                                        link_rule=link_rule, session=session
                                    )

        if self._settings.keep_nulls is False:
            return await self.update(
                SetOperator(self.get_dict()),
                Unset(self._get_top_level_nones()),
                session=session,
                ignore_revision=ignore_revision,
                upsert=True,
                **kwargs,
            )
        else:
            return await self.update(
                SetOperator(self.get_dict()),
                session=session,
                ignore_revision=ignore_revision,
                upsert=True,
                **kwargs,
            )

    @saved_state_needed
    @wrap_with_actions(EventTypes.SAVE_CHANGES)
    async def save_changes(
        self,
        *,
        ignore_revision: bool = False,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ) -> None:
        """
        Save changes.
        State management usage must be turned on

        :param ignore_revision: bool - ignore revision id, if revision is turned on
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :return: None
        """
        await self._validate_self(skip_actions=skip_actions)
        if not self.is_changed:
            return None
        changes = self.get_changes()
        if self._settings.keep_nulls is False:
            return await self.update(
                SetOperator(changes),
                Unset(self._get_top_level_nones()),
                ignore_revision=ignore_revision,
                session=session,
                bulk_writer=bulk_writer,
            )
        else:
            return await self.set(
                changes,  # type: ignore #TODO fix typing
                ignore_revision=ignore_revision,
                session=session,
                bulk_writer=bulk_writer,
            )

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
        if await cls.find(In(cls.id, ids_list)).count() != len(ids_list):
            raise ReplaceError(
                "Some of the documents are not exist in the collection"
            )
        async with BulkWriter(session=session) as bulk_writer:
            for document in documents:
                await document.replace(
                    bulk_writer=bulk_writer, session=session
                )

    @wrap_with_actions(EventTypes.UPDATE)
    @save_state_after
    async def update(
        self,
        *args,
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
        use_revision_id = self._settings.use_revision

        if self.id is not None:
            find_query: Dict[str, Any] = {"_id": self.id}
        else:
            find_query = {"_id": PydanticObjectId()}

        if use_revision_id and not ignore_revision:
            find_query["revision_id"] = self._previous_revision_id

        if use_revision_id:
            arguments.append(SetRevisionId(self.revision_id))
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
                merge_models(self, result)
        return self

    @classmethod
    def update_all(
        cls,
        *args: Union[dict, Mapping],
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

    def set(
        self,
        expression: Dict[Union[ExpressionField, str], Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **kwargs,
    ):
        """
        Set values

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).set({Sample.one: 100})

        ```

        Uses [Set operator](https://roman-right.github.io/beanie/api/operators/update/#set)

        :param expression: Dict[Union[ExpressionField, str], Any] - keys and
        values to set
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            SetOperator(expression),
            session=session,
            bulk_writer=bulk_writer,
            **kwargs,
        )

    def current_date(
        self,
        expression: Dict[Union[ExpressionField, str], Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **kwargs,
    ):
        """
        Set current date

        Uses [CurrentDate operator](https://roman-right.github.io/beanie/api/operators/update/#currentdate)

        :param expression: Dict[Union[ExpressionField, str], Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            CurrentDate(expression),
            session=session,
            bulk_writer=bulk_writer,
            **kwargs,
        )

    def inc(
        self,
        expression: Dict[Union[ExpressionField, str], Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **kwargs,
    ):
        """
        Increment

        Example:

        ```python

        class Sample(Document):
            one: int

        await Document.find(Sample.one == 1).inc({Sample.one: 100})

        ```

        Uses [Inc operator](https://roman-right.github.io/beanie/api/operators/update/#inc)

        :param expression: Dict[Union[ExpressionField, str], Any]
        :param session: Optional[ClientSession] - pymongo session
        :param bulk_writer: Optional[BulkWriter] - bulk writer
        :return: self
        """
        return self.update(
            Inc(expression),
            session=session,
            bulk_writer=bulk_writer,
            **kwargs,
        )

    @wrap_with_actions(EventTypes.DELETE)
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
            link_fields = self.get_link_fields()
            if link_fields is not None:
                for field_info in link_fields.values():
                    value = getattr(self, field_info.field_name)
                    if field_info.link_type in [
                        LinkTypes.DIRECT,
                        LinkTypes.OPTIONAL_DIRECT,
                        LinkTypes.BACK_DIRECT,
                        LinkTypes.OPTIONAL_BACK_DIRECT,
                    ]:
                        if isinstance(value, Document):
                            await value.delete(
                                link_rule=DeleteRules.DELETE_LINKS,
                                **pymongo_kwargs,
                            )
                    if field_info.link_type in [
                        LinkTypes.LIST,
                        LinkTypes.OPTIONAL_LIST,
                        LinkTypes.BACK_LIST,
                        LinkTypes.OPTIONAL_BACK_LIST,
                    ]:
                        if isinstance(value, List):
                            for obj in value:
                                if isinstance(obj, Document):
                                    await obj.delete(
                                        link_rule=DeleteRules.DELETE_LINKS,
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

    def save_state(self) -> None:
        """
        Save current document state. Internal method
        :return: None
        """
        settings = self._settings
        if settings.use_state_management and self.id is not None:
            if settings.state_management_save_previous:
                self._previous_saved_state = self._saved_state
            self._saved_state = self.get_dict()

    @property  # type: ignore
    @saved_state_needed
    def is_changed(self) -> bool:
        return self._saved_state != self.get_dict()

    @property  # type: ignore
    @saved_state_needed
    @previous_saved_state_needed
    def has_changed(self) -> bool:
        return (
            self._previous_saved_state is not None
            and self._previous_saved_state != self._saved_state
        )

    def _collect_updates(
        self, old_dict: Dict[str, Any], new_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compares old_dict with new_dict and returns field paths that have been updated
        Args:
            old_dict: dict1
            new_dict: dict2

        Returns: dictionary with updates
        """
        if old_dict.keys() - new_dict.keys():
            return new_dict
        updates = {}
        replace_objects = self._settings.state_management_replace_objects
        for name, new_value in new_dict.items():
            old_value = old_dict.get(name)
            if new_value == old_value:
                continue
            if (
                not replace_objects
                and isinstance(new_value, dict)
                and isinstance(old_value, dict)
            ):
                value_updates = self._collect_updates(old_value, new_value)
                for k, v in value_updates.items():
                    updates[f"{name}.{k}"] = v
            else:
                updates[name] = new_value
        return updates

    @saved_state_needed
    def get_changes(self) -> Dict[str, Any]:
        return self._collect_updates(self._saved_state, self.get_dict())  # type: ignore

    @saved_state_needed
    @previous_saved_state_needed
    def get_previous_changes(self) -> Dict[str, Any]:
        if self._previous_saved_state is None:
            return {}

        return self._collect_updates(
            self._previous_saved_state, self._saved_state  # type: ignore
        )

    @saved_state_needed
    def rollback(self) -> None:
        if self.is_changed:
            for key, value in self._saved_state.items():  # type: ignore
                if key == "_id":
                    setattr(self, "id", value)
                else:
                    setattr(self, key, value)

    # Other

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

    def dict(
        self,
        *,
        include: Union["AbstractSetIntStr", "MappingIntStrAny"] = None,
        exclude: Union["AbstractSetIntStr", "MappingIntStrAny"] = None,
        by_alias: bool = False,
        skip_defaults: bool = False,
        exclude_hidden: bool = True,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        exclude_none: bool = False,
    ) -> "DictStrAny":
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
                )  # type: ignore
            elif exclude is None:
                exclude = self._hidden_fields

        kwargs = {
            "include": include,
            "exclude": exclude,
            "by_alias": by_alias,
            "exclude_unset": exclude_unset,
            "exclude_defaults": exclude_defaults,
            "exclude_none": exclude_none,
        }

        # TODO: Remove this check when skip_defaults are no longer supported
        if skip_defaults:
            kwargs["skip_defaults"] = skip_defaults

        return self.model_dump(**kwargs)

    def get_dict(
        self,
        to_db: bool = True,
        exclude: Optional[Set[str]] = None,
        keep_nulls: Optional[bool] = None,
    ):
        if exclude is None:
            exclude = set()
        if self.id is None:
            exclude.add("_id")
        if not self._settings.use_revision:
            exclude.add("revision_id")
        if keep_nulls is None:
            keep_nulls = self._settings.keep_nulls
        encoder = Encoder(exclude=exclude, to_db=to_db, keep_nulls=keep_nulls)
        return encoder.encode(self)

    def _get_top_level_nones(self, exclude: Optional[Set[str]] = None):
        dictionary = self.get_dict(
            exclude=exclude, to_db=False, keep_nulls=True
        )
        return {k: v for k, v in dictionary.items() if v is None}

    @wrap_with_actions(event_type=EventTypes.VALIDATE_ON_SAVE)
    async def _validate_self(
        self,
        *,
        skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
    ):
        # TODO: it can be sync, but needs some actions controller improvements
        if self._settings.validate_on_save:
            data = self.model_dump()
            self.__class__.model_validate(data)

    def to_ref(self):
        if self.id is None:
            raise DocumentWasNotSaved("Can not create dbref without id")
        return DBRef(self.get_motor_collection().name, self.id)

    async def fetch_link(self, field: str):
        ref_obj = getattr(self, field, None)
        if isinstance(ref_obj, Link):
            value = await ref_obj.fetch(fetch_links=True)
            setattr(self, field, value)
        if isinstance(ref_obj, list) and ref_obj:
            values = await Link.fetch_list(ref_obj, fetch_links=True)
            setattr(self, field, values)

    async def fetch_all_links(self):
        coros = [  # TODO lists
            self.fetch_link(ref.field_name)
            for ref in self.get_link_fields().values()
        ]
        await asyncio.gather(*coros)

    @classmethod
    async def distinct(
        cls,
        key: str,
        filter: Optional[Mapping[str, Any]] = None,
        session: Optional[ClientSession] = None,
        **kwargs: Any,
    ) -> list:
        return await cls.get_motor_collection().distinct(
            key, filter, session, **kwargs
        )

    @classmethod
    def link_from_id(cls, id: Any):
        ref = DBRef(id=id, collection=cls.get_collection_name())
        return Link(ref, document_class=cls)
