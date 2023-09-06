from collections import OrderedDict
from enum import Enum
from typing import (
    TYPE_CHECKING,
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
    get_args,
    get_origin,
)
from typing import OrderedDict as OrderedDictType

from bson import DBRef
from pydantic import (
    BaseModel,
    GetCoreSchemaHandler,
    TypeAdapter,
    model_validator,
)
from pydantic.fields import FieldInfo
from pydantic_core import core_schema

from beanie.odm.operators.find.comparison import In
from beanie.odm.registry import DocsRegistry
from beanie.odm.utils.parsing import parse_obj

if TYPE_CHECKING:
    from beanie.odm.documents import DocType


DOCS_REGISTRY: DocsRegistry[BaseModel] = DocsRegistry()


class LinkTypes(str, Enum):
    DIRECT = "DIRECT"
    OPTIONAL_DIRECT = "OPTIONAL_DIRECT"
    LIST = "LIST"
    OPTIONAL_LIST = "OPTIONAL_LIST"

    BACK_DIRECT = "BACK_DIRECT"
    BACK_LIST = "BACK_LIST"
    OPTIONAL_BACK_DIRECT = "OPTIONAL_BACK_DIRECT"
    OPTIONAL_BACK_LIST = "OPTIONAL_BACK_LIST"


class LinkInfo(BaseModel):
    field_name: str
    lookup_field_name: str
    document_class: Type[BaseModel]  # Document class
    link_type: LinkTypes
    nested_links: Optional[Dict] = None


T = TypeVar("T")


class Link(Generic[T]):
    def __init__(self, ref: DBRef, document_class: Type[T]):
        self.ref = ref
        self.document_class = document_class

    async def fetch(self, fetch_links: bool = False) -> Union[T, "Link"]:
        result = await self.document_class.get(  # type: ignore
            self.ref.id, with_children=True, fetch_links=fetch_links
        )
        return result or self

    @classmethod
    async def fetch_list(
        cls, links: List[Union["Link", "DocType"]], fetch_links: bool = False
    ):
        """
        Fetch list that contains links and documents
        :param links:
        :param fetch_links:
        :return:
        """
        data = Link.repack_links(links)  # type: ignore
        ids_to_fetch = []
        document_class = None
        for link in data.values():
            if isinstance(link, Link):
                if document_class is None:
                    document_class = link.document_class
                else:
                    if document_class != link.document_class:
                        raise ValueError(
                            "All the links must have the same model class"
                        )
                ids_to_fetch.append(link.ref.id)

        fetched_models = await document_class.find(  # type: ignore
            In("_id", ids_to_fetch),
            with_children=True,
            fetch_links=fetch_links,
        ).to_list()

        for model in fetched_models:
            data[model.id] = model

        return list(data.values())

    @staticmethod
    def repack_links(
        links: List[Union["Link", "DocType"]]
    ) -> OrderedDictType[Any, Any]:
        result = OrderedDict()
        for link in links:
            if isinstance(link, Link):
                result[link.ref.id] = link
            else:
                result[link.id] = link
        return result

    @staticmethod
    def serialize(value: Union["Link", BaseModel]):
        if isinstance(value, Link):
            return value.to_dict()
        return value.model_dump()

    @classmethod
    def build_validation(cls, handler, source_type):
        def validate(v: Union[DBRef, T], _: core_schema.ValidationInfo):
            document_class = DOCS_REGISTRY.evaluate_fr(
                get_args(source_type)[0]
            )

            if isinstance(v, DBRef):
                return cls(ref=v, document_class=document_class)
            if isinstance(v, Link):
                return v
            if isinstance(v, (dict, BaseModel)):
                return parse_obj(document_class, v)
            new_id = TypeAdapter(
                document_class.model_fields["id"].annotation
            ).validate_python(v)
            ref = DBRef(
                collection=document_class.get_collection_name(), id=new_id
            )
            return cls(ref=ref, document_class=document_class)

        return validate

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:  # type: ignore
        return core_schema.json_or_python_schema(
            python_schema=core_schema.general_plain_validator_function(
                cls.build_validation(handler, source_type)
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
            serialization=core_schema.plain_serializer_function_ser_schema(  # type: ignore
                lambda instance: cls.serialize(instance)  # type: ignore
            ),
        )

    def to_ref(self):
        return self.ref

    def to_dict(self):
        return {"id": str(self.ref.id), "collection": self.ref.collection}


class BackLink(Generic[T]):
    """Back reference to a document"""

    def __init__(self, document_class: Type[T]):
        self.document_class = document_class

    @classmethod
    def build_validation(cls, handler, source_type):
        def validate(v: Union[DBRef, T], field):
            document_class = DOCS_REGISTRY.evaluate_fr(
                get_args(source_type)[0]
            )
            if isinstance(v, (dict, BaseModel)):
                return parse_obj(document_class, v)
            return cls(document_class=document_class)

        return validate

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> core_schema.CoreSchema:  # type: ignore
        return core_schema.general_plain_validator_function(
            cls.build_validation(handler, source_type)
        )

    def to_dict(self):
        return {"collection": self.document_class.get_collection_name()}


class LinkedModel(BaseModel):
    _link_fields: ClassVar[Dict[str, LinkInfo]]

    @classmethod
    def get_link_fields(cls) -> Dict[str, LinkInfo]:
        try:
            # _link_fields is not inheritable
            return cls.__dict__["_link_fields"]
        except KeyError:
            cls._link_fields = {}
            for k, v in cls.model_fields.items():
                link_info = detect_link(v, k)
                if link_info is not None:
                    cls._link_fields[k] = link_info
                    check_nested_links(link_info)
            return cls._link_fields

    @model_validator(mode="before")
    @classmethod
    def _fill_back_refs(cls, values):
        for field_name, link_info in cls.get_link_fields().items():
            if (
                link_info.link_type
                in [LinkTypes.BACK_DIRECT, LinkTypes.OPTIONAL_BACK_DIRECT]
                and field_name not in values
            ):
                values[field_name] = BackLink[link_info.document_class](
                    link_info.document_class
                )
            if (
                link_info.link_type
                in [LinkTypes.BACK_LIST, LinkTypes.OPTIONAL_BACK_LIST]
                and field_name not in values
            ):
                values[field_name] = [
                    BackLink[link_info.document_class](
                        link_info.document_class
                    )
                ]
        return values


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
