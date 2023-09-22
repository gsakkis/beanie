from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Type,
    Union,
    cast,
)

import pymongo
from pymongo.client_session import ClientSession
from pymongo.results import UpdateResult
from typing_extensions import Self

import beanie
from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.fields import ExpressionField
from beanie.odm.interfaces.update import UpdateMethods
from beanie.odm.operators import FieldName
from beanie.odm.queries import BaseQuery
from beanie.odm.utils.parsing import parse_obj


class UpdateResponse(str, Enum):
    UPDATE_RESULT = "UPDATE_RESULT"  # PyMongo update result
    OLD_DOCUMENT = "OLD_DOCUMENT"  # Original document
    NEW_DOCUMENT = "NEW_DOCUMENT"  # Updated document


UpdateExpression = Union[Mapping[FieldName, Any], List[Any]]


@dataclass
class UpdateQuery(BaseQuery, UpdateMethods):
    """Update Query base class"""

    find_query: Mapping[str, Any] = field(default_factory=dict)
    bulk_writer: Optional[BulkWriter] = None
    on_insert: Optional["beanie.Document"] = None
    update_expressions: List[UpdateExpression] = field(
        default_factory=list, init=False
    )

    @property
    def update_query(self) -> Dict[str, Any]:
        if not self.update_expressions:
            raise ValueError("No update expressions provided")

        query: Union[Dict[str, Any], List[Mapping[str, Any]]]
        if isinstance(self.update_expressions[0], list):
            query = []
            for expression in self.update_expressions:
                query.extend(expression)
        else:
            query = {}
            for expression in self.update_expressions:
                expression = cast(Dict[str, Any], expression)
                if (
                    "$set" in expression
                    and "revision_id" in expression["$set"]
                ):
                    query.setdefault("$set", {}).update(expression["$set"])
                else:
                    query.update(expression)
        return self.encoder.encode(query)

    def _add_update_expression(self, expression: UpdateExpression) -> None:
        if not isinstance(expression, (Mapping, list)):
            raise TypeError("Update expression must be dict or list")
        if self.update_expressions:
            expr_type = type(self.update_expressions[0])
            assert expr_type in (dict, list)
            if (expr_type is dict and not isinstance(expression, Mapping)) or (
                expr_type is list and not isinstance(expression, list)
            ):
                raise TypeError(
                    "Update expressions must be all lists or all dicts"
                )
        self.update_expressions.append(ExpressionField.serialize(expression))


class UpdateMany(UpdateQuery):
    """Update Many query class"""

    def update(
        self,
        *args: UpdateExpression,
        on_insert: Optional["beanie.Document"] = None,
        bulk_writer: Optional[BulkWriter] = None,
        session: Optional[ClientSession] = None,
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
        for arg in args:
            self._add_update_expression(arg)
        if on_insert is not None:
            self.on_insert = on_insert
        if bulk_writer:
            self.bulk_writer = bulk_writer
        self.pymongo_kwargs.update(pymongo_kwargs)
        return self

    def __await__(
        self,
    ) -> Generator[None, None, Union[UpdateResult, "beanie.Document", None]]:
        return self._update().__await__()

    async def _update(self) -> Union[UpdateResult, "beanie.Document", None]:
        document_model = cast(Type[beanie.Document], self.document_model)
        if self.bulk_writer is not None:
            return self.bulk_writer.add_operation(
                Operation(
                    operation_class=pymongo.UpdateMany,
                    first_query=self.find_query,
                    second_query=self.update_query,
                    object_class=document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
        result: Union[UpdateResult, beanie.Document, None]
        result = await document_model.get_motor_collection().update_many(
            self.find_query,
            self.update_query,
            session=self.session,
            **self.pymongo_kwargs,
        )
        if (
            self.on_insert is not None
            and result is not None
            and result.matched_count == 0
        ):
            result = await document_model.insert_one(
                document=self.on_insert,
                session=self.session,
                bulk_writer=self.bulk_writer,
            )
        return result


@dataclass
class UpdateOne(UpdateQuery):
    """Update One query class"""

    response_type: UpdateResponse = UpdateResponse.UPDATE_RESULT

    def update(
        self,
        *args: UpdateExpression,
        on_insert: Optional["beanie.Document"] = None,
        bulk_writer: Optional[BulkWriter] = None,
        response_type: Optional[UpdateResponse] = None,
        session: Optional[ClientSession] = None,
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
        for arg in args:
            self._add_update_expression(arg)
        if on_insert is not None:
            self.on_insert = on_insert
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
        document_model = cast(Type[beanie.Document], self.document_model)
        if self.bulk_writer:
            return self.bulk_writer.add_operation(
                Operation(
                    operation_class=pymongo.UpdateOne,
                    first_query=self.find_query,
                    second_query=self.update_query,
                    object_class=document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )

        result: Union[UpdateResult, beanie.Document, None] = None
        collection = document_model.get_motor_collection()
        if self.response_type == UpdateResponse.UPDATE_RESULT:
            result = await collection.update_one(
                self.find_query,
                self.update_query,
                session=self.session,
                **self.pymongo_kwargs,
            )
        else:
            r_dict = await collection.find_one_and_update(
                self.find_query,
                self.update_query,
                session=self.session,
                return_document=(
                    pymongo.ReturnDocument.BEFORE
                    if self.response_type == UpdateResponse.OLD_DOCUMENT
                    else pymongo.ReturnDocument.AFTER
                ),
                **self.pymongo_kwargs,
            )
            if r_dict is not None:
                result = cast(
                    beanie.Document, parse_obj(document_model, r_dict)
                )

        if (
            self.on_insert is not None
            and self.response_type == UpdateResponse.UPDATE_RESULT
            and result is not None
            and result.matched_count == 0
        ) or (
            self.on_insert is not None
            and self.response_type != UpdateResponse.UPDATE_RESULT
            and result is None
        ):
            result = await document_model.insert_one(
                document=self.on_insert,
                session=self.session,
                bulk_writer=self.bulk_writer,
            )

        return result
