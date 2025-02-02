import asyncio
import typing
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Optional,
    Set,
    Type,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
)

from bson import DBRef
from pydantic import BaseModel, TypeAdapter, model_validator
from pydantic.fields import FieldInfo
from pydantic_core import core_schema
from typing_extensions import Self

import beanie
from beanie.odm.fields import ExpressionField
from beanie.odm.operators import FieldName
from beanie.odm.operators.comparison import In
from beanie.odm.utils.parsing import parse_obj


class LinkTypes(str, Enum):
    DIRECT = "DIRECT"
    OPTIONAL_DIRECT = "OPTIONAL_DIRECT"
    LIST = "LIST"
    OPTIONAL_LIST = "OPTIONAL_LIST"

    BACK_DIRECT = "BACK_DIRECT"
    OPTIONAL_BACK_DIRECT = "OPTIONAL_BACK_DIRECT"
    BACK_LIST = "BACK_LIST"
    OPTIONAL_BACK_LIST = "OPTIONAL_BACK_LIST"

    @property
    def is_direct(self) -> bool:
        return self.value.endswith("DIRECT")

    @property
    def is_list(self) -> bool:
        return self.value.endswith("LIST")

    @property
    def is_back(self) -> bool:
        return "BACK" in self.value


T = TypeVar("T", bound="beanie.Document")


class Link(Generic[T]):
    def __init__(self, ref: DBRef, document_class: Type[T]):
        self.ref = ref
        self.document_class = document_class

    @classmethod
    def from_document_id(
        cls,
        document_id: Any,
        document_class: Type[T],
        collection: Optional[str] = None,
    ) -> Self:
        if collection is None:
            collection = document_class.get_collection_name()
        id_type = document_class.model_fields["id"].annotation
        valid_id = TypeAdapter(id_type).validate_python(document_id)
        return cls(DBRef(collection, valid_id), document_class)

    async def fetch(self, fetch_links: bool = False) -> Union[T, "Link[T]"]:
        result = await self.document_class.get(
            self.ref.id, with_children=True, fetch_links=fetch_links
        )
        return result or self

    @classmethod
    async def fetch_list(
        cls,
        links: List[Union["Link[T]", "beanie.Document"]],
        fetch_links: bool = False,
    ) -> List[Union["Link[T]", "beanie.Document"]]:
        """Fetch list that contains links and documents"""
        data = OrderedDict()
        for link in links:
            key = link.ref.id if isinstance(link, Link) else link.id
            data[key] = link

        ids_to_fetch = []
        document_class: Optional[Type[T]] = None
        for link in data.values():
            if isinstance(link, Link):
                if document_class is None:
                    document_class = link.document_class
                elif document_class != link.document_class:
                    raise ValueError(
                        "All the links must have the same model class"
                    )
                ids_to_fetch.append(link.ref.id)

        if ids_to_fetch and document_class is not None:
            fetched_models: list[T] = await document_class.find_many(
                In("_id", ids_to_fetch),
                with_children=True,
                fetch_links=fetch_links,
            ).to_list()
            for model in fetched_models:
                data[model.id] = model

        return list(data.values())

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any
    ) -> core_schema.CoreSchema:
        document_annotation = get_args(source_type)[0]

        def validate(v: Union[DBRef, T]) -> Union[Link[T], T]:
            document_class = LinkedModelMixin.eval_type(document_annotation)
            if isinstance(v, DBRef):
                return cls(v, document_class)
            if isinstance(v, Link):
                return v
            if isinstance(v, dict) and v.keys() == {"id", "collection"}:
                return cls.from_document_id(
                    v["id"], document_class, collection=v["collection"]
                )
            if isinstance(v, (dict, BaseModel)):
                return cast(T, parse_obj(document_class, v))
            return cls.from_document_id(v, document_class)

        return core_schema.json_or_python_schema(
            python_schema=core_schema.no_info_plain_validator_function(
                validate
            ),
            json_schema=core_schema.typed_dict_schema(
                {
                    "id": core_schema.typed_dict_field(
                        core_schema.str_schema()
                    ),
                    "collection": core_schema.typed_dict_field(
                        core_schema.str_schema()
                    ),
                }
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(
                lambda v: (
                    v.to_dict() if isinstance(v, Link) else v.model_dump()
                )
            ),
        )

    def to_ref(self) -> DBRef:
        return self.ref

    def to_dict(self) -> Dict[str, str]:
        return {"id": str(self.ref.id), "collection": self.ref.collection}


class BackLink(Generic[T]):
    """Back reference to a document"""

    def __init__(self, document_class: Type[T]):
        self.document_class = document_class

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any
    ) -> core_schema.CoreSchema:
        document_annotation = get_args(source_type)[0]

        def validate(v: Union[DBRef, T]) -> Union[BackLink[T], T]:
            document_class = LinkedModelMixin.eval_type(document_annotation)
            if isinstance(v, (dict, BaseModel)):
                return cast(T, parse_obj(document_class, v))
            return cls(document_class)

        return core_schema.no_info_plain_validator_function(validate)

    def to_dict(self) -> Dict[str, str]:
        return {"collection": self.document_class.get_collection_name()}


@dataclass
class LinkInfo:
    field_name: str
    lookup_field_name: str
    document_class: Type["beanie.Document"]
    link_type: LinkTypes
    nested_links: Optional[Dict[str, "LinkInfo"]] = None

    def __post_init__(self) -> None:
        self.document_class = LinkedModelMixin.eval_type(self.document_class)

    def iter_pipeline_stages(self) -> typing.Iterator[Dict[str, Any]]:
        as_field = self.field_name
        is_direct = self.link_type.is_direct
        if is_direct:
            as_field = "_link_" + as_field

        lookup: Dict[str, Any] = {
            "from": self.document_class.get_collection_name(),
            "as": as_field,
        }

        lookup_field = f"{self.lookup_field_name}.$id"
        is_backlink = self.link_type.is_back
        if beanie.DATABASE_MAJOR_VERSION >= 5 or self.nested_links is None:
            local_field, foreign_field = lookup_field, "_id"
            if is_backlink:
                local_field, foreign_field = foreign_field, local_field
            lookup["localField"] = local_field
            lookup["foreignField"] = foreign_field
        else:
            match_op = "$eq" if is_direct else "$in"
            if is_backlink:
                let_expr = {"link_id": "$_id"}
                match_expr = ["$$link_id", "$" + lookup_field]
            else:
                let_expr = {"link_id": "$" + lookup_field}
                match_expr = ["$_id", "$$link_id"]
            lookup["let"] = let_expr
            lookup["pipeline"] = [
                {"$match": {"$expr": {match_op: match_expr}}}
            ]

        if self.nested_links is not None:
            pipeline = lookup.setdefault("pipeline", [])
            for nested_link_info in self.nested_links.values():
                pipeline.extend(nested_link_info.iter_pipeline_stages())

        yield {"$lookup": lookup}

        if is_direct:
            yield {
                "$unwind": {
                    "path": "$" + as_field,
                    "preserveNullAndEmptyArrays": True,
                }
            }
            yield {
                "$set": {
                    self.field_name: {
                        "$cond": {
                            "if": {"$ifNull": ["$" + as_field, False]},
                            "then": "$" + as_field,
                            "else": "$" + self.field_name,
                        }
                    }
                }
            }
            yield {"$unset": as_field}


class LinkedModelMixin:
    _registry: ClassVar[Dict[str, Type["LinkedModelMixin"]]] = {}
    link_fields: ClassVar[Dict[str, LinkInfo]]

    @classmethod
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls._registry[cls.__name__] = cls

    @classmethod
    def get_link_fields(cls) -> Dict[str, LinkInfo]:
        try:
            # link_fields is not inheritable
            link_fields: Dict[str, LinkInfo] = cls.__dict__["link_fields"]
        except KeyError:
            if not issubclass(cls, BaseModel):
                raise TypeError("LinkedModelMixin must be used with BaseModel")
            # this can't be done in __init_subclass__ because there may forward
            # references to models that are not registered yet
            cls.link_fields = link_fields = {}
            for k, v in cls.model_fields.items():
                link_info = detect_link(v, k)
                if link_info is not None:
                    link_fields[k] = link_info
                    check_nested_links(link_info)
        return link_fields

    @classmethod
    def eval_type(cls, t: Any) -> Type["beanie.Document"]:
        return typing._eval_type(t, cls._registry, None)  # type: ignore

    async def fetch_link(self, field: FieldName) -> None:
        if isinstance(field, ExpressionField):
            attr = str(field)
        else:
            assert isinstance(field, str)
            attr = field
        ref_obj = getattr(self, attr, None)
        if isinstance(ref_obj, Link):
            value = await ref_obj.fetch(fetch_links=True)
            setattr(self, attr, value)
        elif isinstance(ref_obj, list) and ref_obj:
            values = await Link.fetch_list(ref_obj, fetch_links=True)
            setattr(self, attr, values)

    async def fetch_all_links(self) -> None:
        await asyncio.gather(
            *(
                self.fetch_link(ref.field_name)
                for ref in self.get_link_fields().values()
            )
        )

    @model_validator(mode="before")
    @classmethod
    def _fill_back_refs(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        for field_name, link_info in cls.get_link_fields().items():
            if field_name not in values and link_info.link_type.is_back:
                backlink = BackLink(link_info.document_class)
                if link_info.link_type.is_list:
                    values[field_name] = [backlink]
                else:
                    values[field_name] = backlink
        return values


def detect_link(field_info: FieldInfo, field_name: str) -> Optional[LinkInfo]:
    """It detects link and returns LinkInfo if any found"""
    annotation = field_info.annotation
    origin = get_origin(annotation)
    args = get_args(annotation)
    for cls in Link, BackLink:
        lookup_field_name = None
        if cls is Link:
            lookup_field_name = field_name
        elif isinstance(field_info.json_schema_extra, dict):
            lookup_field_name = cast(
                str, field_info.json_schema_extra.get("original_field")
            )
        if lookup_field_name is None:
            continue

        # Check if annotation is one of the custom classes
        if origin is cls:
            link_type = LinkTypes["DIRECT" if cls is Link else "BACK_DIRECT"]
            return LinkInfo(
                field_name=field_name,
                lookup_field_name=lookup_field_name,
                document_class=args[0],
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
                document_class=get_args(args[0])[0],
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
                document_class=optional_args[0],
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
                document_class=get_args(optional_args[0])[0],
                link_type=link_type,
            )

    return None


def check_nested_links(
    link_info: LinkInfo, checked: Optional[Set[Type[BaseModel]]] = None
) -> None:
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
