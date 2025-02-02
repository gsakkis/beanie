from functools import partial
from typing import Any, Generator, Generic, Optional, Type, TypeVar

import pymongo
from pydantic import BaseModel
from pymongo.client_session import ClientSession
from pymongo.results import UpdateResult
from typing_extensions import Self

import beanie
from beanie.exceptions import DocumentNotFound
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.fields import ExpressionField
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.queries import FieldNameMapping
from beanie.odm.queries.delete import DeleteOne
from beanie.odm.queries.find_many import FindMany
from beanie.odm.queries.find_query import FindQuery, get_projection
from beanie.odm.queries.update import UpdateOne, UpdateResponse
from beanie.odm.utils.parsing import ParseableModel, parse_obj

ModelT = TypeVar("ModelT", bound=BaseModel)


class FindOne(FindQuery, UpdateMethods, Generic[ModelT]):
    """Find One query class"""

    projection_model: Type[ParseableModel]

    def find(
        self,
        *args: FieldNameMapping,
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
        self.find_expressions.extend(map(ExpressionField.serialize, args))
        self.project(projection_model)
        self.set_session(session)
        self.ignore_cache = ignore_cache
        self.fetch_links = fetch_links
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def update(
        self,
        *args: FieldNameMapping,
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
            bulk_writer=bulk_writer,
            session=self.session,
            pymongo_kwargs=pymongo_kwargs,
        ).update(*args, response_type=response_type)

    def upsert(
        self,
        *args: FieldNameMapping,
        on_insert: "beanie.Document",
        session: Optional[ClientSession] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> UpdateOne:
        """
        Create Update with modifications query
        and provide search criteria there

        :param args: *Mapping[str,Any] - the modifications to apply.
        :param on_insert: Document - document to insert if there is no matched
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
            on_insert=on_insert,
            session=self.session,
            pymongo_kwargs=pymongo_kwargs,
        ).update(*args, response_type=response_type)

    def delete(
        self,
        bulk_writer: Optional[BulkWriter] = None,
        session: Optional[ClientSession] = None,
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
            bulk_writer=bulk_writer,
            session=self.session,
            pymongo_kwargs=pymongo_kwargs,
        )

    async def replace(
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
            result = (
                await self.document_model.get_motor_collection().replace_one(
                    self.get_filter_query(),
                    document.get_dict(exclude={"_id"}),
                    session=self.session,
                )
            )
            if result.raw_result and not result.raw_result["updatedExisting"]:
                raise DocumentNotFound
            return result
        else:
            bulk_writer.add_operation(
                Operation(
                    operation_class=pymongo.ReplaceOne,
                    first_query=self.get_filter_query(),
                    second_query=document.get_dict(exclude={"_id"}),
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
            return None

    def __await__(self) -> Generator[None, None, Optional[ModelT]]:
        return self._find().__await__()

    async def _find(self, use_cache: bool = True) -> Any:
        projection_model = self.projection_model
        if use_cache:
            cache = self.document_model.get_cache()
            if cache is None or self.ignore_cache:
                return await self._find(use_cache=False)
            doc = await cache.get(
                self._cache_key, partial(self._find, use_cache=False)
            )
        elif self.fetch_links:
            find_many = FindMany[ModelT](self.document_model)
            doc = await find_many.find(
                *self.find_expressions,
                session=self.session,
                fetch_links=self.fetch_links,
                projection_model=projection_model,
                **self.pymongo_kwargs,
            ).first_or_none()
        else:
            doc = await self.document_model.get_motor_collection().find_one(
                filter=self.get_filter_query(),
                projection=get_projection(projection_model),
                session=self.session,
                **self.pymongo_kwargs,
            )

        if doc is not None and not isinstance(doc, projection_model):
            doc = parse_obj(projection_model, doc)
        return doc
