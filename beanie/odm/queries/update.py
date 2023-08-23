from enum import Enum
from typing import (
    TYPE_CHECKING,
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
from pymongo.results import InsertOneResult, UpdateResult
from typing_extensions import Self

from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.operators.update import BaseUpdateOperator
from beanie.odm.operators.update.general import SetRevisionId
from beanie.odm.queries import BaseQuery
from beanie.odm.utils.encoder import Encoder
from beanie.odm.utils.parsing import parse_obj

if TYPE_CHECKING:
    from beanie.odm.documents import DocType


class UpdateResponse(str, Enum):
    UPDATE_RESULT = "UPDATE_RESULT"  # PyMongo update result
    OLD_DOCUMENT = "OLD_DOCUMENT"  # Original document
    NEW_DOCUMENT = "NEW_DOCUMENT"  # Updated document


class UpdateQuery(BaseQuery, UpdateMethods):
    """Update Query base class"""

    def __init__(
        self, document_model: Type["DocType"], find_query: Mapping[str, Any]
    ):
        super().__init__()
        self.document_model = document_model
        self.find_query = find_query
        self.update_expressions: List[Mapping[str, Any]] = []
        self.is_upsert = False
        self.upsert_insert_doc: Optional["DocType"] = None
        self.encoders: Dict[Any, Callable[[Any], Any]] = {}
        self.bulk_writer: Optional[BulkWriter] = None
        self.encoders = self.document_model.get_settings().bson_encoders
        self.pymongo_kwargs: Dict[str, Any] = {}

    @property
    def update_query(self) -> Dict[str, Any]:
        query: Dict[str, Any] = {}
        for expression in self.update_expressions:
            if isinstance(expression, BaseUpdateOperator):
                query.update(expression.query)
            elif isinstance(expression, dict):
                query.update(expression)
            elif isinstance(expression, SetRevisionId):
                set_query = query.get("$set", {})
                set_query.update(expression.query.get("$set", {}))
                query["$set"] = set_query
            else:
                raise TypeError("Wrong expression type")
        return Encoder(custom_encoders=self.encoders).encode(query)


class UpdateMany(UpdateQuery):
    """Update Many query class"""

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the update query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[ClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        self.set_session(session)
        self.update_expressions += args
        if bulk_writer:
            self.bulk_writer = bulk_writer
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "DocType",
        session: Optional[ClientSession] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the upsert query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :param **pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        self.upsert_insert_doc = on_insert  # type: ignore
        self.update(*args, session=session, **pymongo_kwargs)
        return self

    def update_many(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the update query

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[ClientSession]
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        return self.update(
            *args, session=session, bulk_writer=bulk_writer, **pymongo_kwargs
        )

    async def _update(self) -> Optional[UpdateResult]:
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

    def __await__(
        self,
    ) -> Generator[None, None, Union[UpdateResult, InsertOneResult]]:
        return self._update().__await__()


class UpdateOne(UpdateQuery):
    """Update One query class"""

    def __init__(
        self, document_model: Type["DocType"], find_query: Mapping[str, Any]
    ):
        super().__init__(document_model, find_query)
        self.response_type = UpdateResponse.UPDATE_RESULT

    def update(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the update query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[ClientSession]
        :param bulk_writer: Optional[BulkWriter]
        :param response_type: UpdateResponse
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        self.set_session(session)
        self.update_expressions += args
        if response_type is not None:
            self.response_type = response_type
        if bulk_writer:
            self.bulk_writer = bulk_writer
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def upsert(
        self,
        *args: Mapping[str, Any],
        on_insert: "DocType",
        session: Optional[ClientSession] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the upsert query.

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param on_insert: DocType - document to insert if there is no matched
        document in the collection
        :param session: Optional[ClientSession]
        :param response_type: Optional[UpdateResponse]
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        self.upsert_insert_doc = on_insert  # type: ignore
        self.update(
            *args,
            response_type=response_type,
            session=session,
            **pymongo_kwargs,
        )
        return self

    def update_one(
        self,
        *args: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        **pymongo_kwargs: Any,
    ) -> Self:
        """
        Provide modifications to the update query. The same as `update()`

        :param args: *Union[dict, Mapping] - the modifications to apply.
        :param session: Optional[ClientSession]
        :param bulk_writer: "BulkWriter" - Beanie bulk writer
        :param response_type: Optional[UpdateResponse]
        :param pymongo_kwargs: pymongo native parameters for update operation
        :return: self
        """
        return self.update(
            *args,
            session=session,
            bulk_writer=bulk_writer,
            response_type=response_type,
            **pymongo_kwargs,
        )

    async def _update(
        self,
    ) -> Union[InsertOneResult, UpdateResult, "DocType", None]:
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

    def __await__(
        self,
    ) -> Generator[
        None, None, Union[InsertOneResult, UpdateResult, "DocType", None]
    ]:
        return self._update().__await__()
