from typing import (
    TYPE_CHECKING,
    Any,
    Generator,
    Generic,
    Mapping,
    Optional,
    Type,
    TypeVar,
    Union,
    cast,
)

from pydantic import BaseModel
from pymongo import ReplaceOne
from pymongo.client_session import ClientSession
from pymongo.results import UpdateResult
from typing_extensions import Self

import beanie
from beanie.exceptions import DocumentNotFound
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.queries.delete import DeleteOne
from beanie.odm.queries.update import UpdateOne, UpdateResponse
from beanie.odm.utils.dump import get_dict
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import ParseableModel, parse_obj

from .base import FindQuery

if TYPE_CHECKING:
    from beanie.odm.interfaces.find import FindInterface


ModelT = TypeVar("ModelT", bound=BaseModel)


class FindOne(FindQuery, UpdateMethods, Generic[ModelT]):
    """Find One query class"""

    projection_model: Type[ModelT]

    def __init__(self, document_model: Type["FindInterface"]):
        projection_model = cast(Type[ParseableModel], document_model)
        super().__init__(document_model, projection_model)

    def find(
        self,
        *args: Union[Mapping[str, Any], bool],
        projection_model: Optional[Type[ParseableModel]] = None,
        session: Optional[ClientSession] = None,
        ignore_cache: bool = False,
        fetch_links: bool = False,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Find one document by criteria

        :param args: *Mapping[str, Any] - search criteria
        :param projection_model: Optional[Type[BaseModel]] - projection model
        :param session: Optional[ClientSession] - pymongo session
        :param ignore_cache: bool
        :param **pymongo_kwargs: pymongo native parameters for find operation (if Document class contains links, this parameter must fit the respective parameter of the aggregate MongoDB function)
        :return: FindOne - query instance
        """
        self.find_expressions += args  # type: ignore # bool workaround
        self.project(projection_model)
        self.set_session(session)
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateOne:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param session: Optional[ClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param response_type: Optional[UpdateResponse]
        :return: UpdateOne query
        """
        if not issubclass(self.document_model, beanie.Document):
            raise RuntimeError("can update only beanie.Document")
        self.set_session(session)
        return UpdateOne(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
        ).update(
            *args,
            session=self.session,
            bulk_writer=bulk_writer,
            response_type=response_type,
            **pymongo_kwargs,
        )

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "beanie.Document",
        session: Optional[ClientSession] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateOne:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :param response_type: Optional[UpdateResponse]
        :return: UpdateOne query
        """
        if not issubclass(self.document_model, beanie.Document):
            raise RuntimeError("can upsert only beanie.Document")
        self.set_session(session)
        return UpdateOne(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
        ).upsert(
            *args,
            on_insert=on_insert,
            session=self.session,
            response_type=response_type,
            **pymongo_kwargs,
        )

    def delete(
        self,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> DeleteOne:
        """
        Provide search criteria to the Delete query

        :param session: Optional[ClientSession]
        :return: Union[DeleteOne, DeleteMany]
        """
        self.set_session(session)
        return DeleteOne(
            document_model=self.document_model,
            find_query=self.get_filter_query(),
            session=self.session,
            bulk_writer=bulk_writer,
            **pymongo_kwargs,
        )

    async def replace_one(
        self,
        document: "beanie.Document",
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
    ) -> Optional[UpdateResult]:
        """
        Replace found document by provided
        :param document: Document - document, which will replace the found one
        :param session: Optional[ClientSession] - PyMongo session
        :param bulk_writer: Optional[BulkWriter] - Beanie bulk writer
        :return: UpdateResult
        """
        self.set_session(session)
        if bulk_writer is None:
            result: UpdateResult = (
                await self.document_model.get_motor_collection().replace_one(
                    self.get_filter_query(),
                    get_dict(
                        document,
                        to_db=True,
                        exclude={"_id"},
                        keep_nulls=document.get_settings().keep_nulls,
                    ),
                    session=self.session,
                )
            )

            if not result.raw_result["updatedExisting"]:
                raise DocumentNotFound
            return result
        else:
            bulk_writer.add_operation(
                Operation(
                    operation=ReplaceOne,
                    first_query=self.get_filter_query(),
                    second_query=Encoder(exclude={"_id"}).encode(document),
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
            return None

    async def count(self) -> int:
        """
        Count the number of documents matching the query
        :return: int
        """
        if self.fetch_links:
            return await self.document_model.find_many(
                *self.find_expressions,
                session=self.session,
                fetch_links=self.fetch_links,
                **self.pymongo_kwargs,
            ).count()
        return await super().count()

    def __await__(self) -> Generator[None, None, Optional[ModelT]]:
        return self._find_one(use_cache=True, parse=True).__await__()

    async def _find_one(
        self, use_cache: bool, parse: bool
    ) -> Optional[ModelT]:
        if use_cache:
            cache = self._cache
            if cache is None:
                return await self._find_one(use_cache=False, parse=parse)

            cache_key = self._cache_key
            doc = cache.get(cache_key)
            if doc is None:
                doc = await self._find_one(use_cache=False, parse=False)
                cache.set(cache_key, doc)
        elif self.fetch_links:
            doc = await self.document_model.find_many(
                *self.find_expressions,
                session=self.session,
                fetch_links=self.fetch_links,
                projection_model=self.projection_model,
                **self.pymongo_kwargs,
            ).first_or_none()
        else:
            doc = await self.document_model.get_motor_collection().find_one(
                filter=self.get_filter_query(),
                projection=self._get_projection(),
                session=self.session,
                **self.pymongo_kwargs,
            )

        if not parse or doc is None or isinstance(doc, self.projection_model):
            return doc
        return cast(ModelT, parse_obj(self.projection_model, doc))
