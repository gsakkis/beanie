from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Type,
    Union,
)

from pymongo import ReturnDocument
from pymongo import UpdateMany as UpdateManyPyMongo
from pymongo import UpdateOne as UpdateOnePyMongo
from pymongo.client_session import ClientSession
from pymongo.results import UpdateResult
from typing_extensions import Self

import beanie
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.operators import BaseOperator
from beanie.odm.operators.update import SetRevisionId
from beanie.odm.queries import BaseQuery
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import parse_obj


class UpdateResponse(str, Enum):
    UPDATE_RESULT = "UPDATE_RESULT"  # PyMongo update result
    OLD_DOCUMENT = "OLD_DOCUMENT"  # Original document
    NEW_DOCUMENT = "NEW_DOCUMENT"  # Updated document


class UpdateQuery(BaseQuery, UpdateMethods):
    """Update Query base class"""

    def __init__(
        self,
        document_model: Type["beanie.Document"],
        find_query: Mapping[str, Any],
    ):
        super().__init__()
        self.document_model = document_model
        self.find_query = find_query
        self.update_expressions: List[Mapping[str, Any]] = []
        self.upsert_insert_doc: Optional[beanie.Document] = None
        self.encoders: Dict[Any, Callable[[Any], Any]] = {}
        self.bulk_writer: Optional[BulkWriter] = None
        self.encoders = self.document_model.get_settings().bson_encoders

    @property
    def update_query(self) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        for expression in self.update_expressions:
            if isinstance(expression, SetRevisionId):
                query.setdefault("$set", {}).update(expression.query["$set"])
            elif isinstance(expression, BaseOperator):
                query.update(expression.query)
            elif isinstance(expression, dict):
                query.update(expression)
            else:
                raise TypeError("Wrong expression type")
        return Encoder(custom_encoders=self.encoders).encode(query)


class UpdateMany(UpdateQuery):
    """Update Many query class"""

    def update(
        self,
        *args: Mapping[str, Any],
        on_insert: Optional["beanie.Document"] = None,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the update query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param on_insert: Document - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        self.set_session(session)
        self.update_expressions += args
        if on_insert is not None:
            self.upsert_insert_doc = on_insert
        if bulk_writer:
            self.bulk_writer = bulk_writer
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def __await__(
        self,
    ) -> Generator[None, None, Union[UpdateResult, "beanie.Document", None]]:
        return self._update().__await__()

    async def _update(self) -> Union[UpdateResult, "beanie.Document", None]:
        if self.bulk_writer is not None:
            return self.bulk_writer.add_operation(
                Operation(
                    operation=UpdateManyPyMongo,
                    first_query=self.find_query,
                    second_query=self.update_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
        result = await self.document_model.get_motor_collection().update_many(
            self.find_query,
            self.update_query,
            session=self.session,
            **self.pymongo_kwargs,
        )
        if (
            self.upsert_insert_doc is not None
            and result is not None
            and result.matched_count == 0
        ):
            result = await self.document_model.insert_one(
                document=self.upsert_insert_doc,
                session=self.session,
                bulk_writer=self.bulk_writer,
            )
        return result


class UpdateOne(UpdateQuery):
    """Update One query class"""

    response_type: UpdateResponse = UpdateResponse.UPDATE_RESULT

    def update(
        self,
        *args: Mapping[str, Any],
        on_insert: Optional["beanie.Document"] = None,
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the update query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param on_insert: Document - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param response_type: UpdateResponse
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        self.set_session(session)
        self.update_expressions += args
        if on_insert is not None:
            self.upsert_insert_doc = on_insert
        if response_type is not None:
            self.response_type = response_type
        if bulk_writer:
            self.bulk_writer = bulk_writer
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def __await__(
        self,
    ) -> Generator[None, None, Union[UpdateResult, "beanie.Document", None]]:
        return self._update().__await__()

    async def _update(self) -> Union[UpdateResult, "beanie.Document", None]:
        if self.bulk_writer:
            return self.bulk_writer.add_operation(
                Operation(
                    operation=UpdateOnePyMongo,
                    first_query=self.find_query,
                    second_query=self.update_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )

        collection = self.document_model.get_motor_collection()
        if self.response_type == UpdateResponse.UPDATE_RESULT:
            result = await collection.update_one(
                self.find_query,
                self.update_query,
                session=self.session,
                **self.pymongo_kwargs,
            )
        else:
            result = await collection.find_one_and_update(
                self.find_query,
                self.update_query,
                session=self.session,
                return_document=(
                    ReturnDocument.BEFORE
                    if self.response_type == UpdateResponse.OLD_DOCUMENT
                    else ReturnDocument.AFTER
                ),
                **self.pymongo_kwargs,
            )
            if result is not None:
                result = parse_obj(self.document_model, result)

        if (
            self.upsert_insert_doc is not None
            and self.response_type == UpdateResponse.UPDATE_RESULT
            and result is not None
            and result.matched_count == 0
        ) or (
            self.upsert_insert_doc is not None
            and self.response_type != UpdateResponse.UPDATE_RESULT
            and result is None
        ):
            result = await self.document_model.insert_one(
                document=self.upsert_insert_doc,
                session=self.session,
                bulk_writer=self.bulk_writer,
            )

        return result
