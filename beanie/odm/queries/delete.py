from typing import TYPE_CHECKING, Any, Dict, Generator, Mapping, Optional, Type

from pymongo import DeleteMany as DeleteManyPyMongo
from pymongo import DeleteOne as DeleteOnePyMongo
from pymongo.client_session import ClientSession
from pymongo.results import DeleteResult

from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.queries import BaseQuery

if TYPE_CHECKING:
    from beanie.odm.interfaces.find import FindInterface


class DeleteQuery(BaseQuery):
    """Deletion Query"""

    def __init__(
        self,
        document_model: Type["FindInterface"],
        find_query: Mapping[str, Any],
        session: Optional[ClientSession] = None,
        bulk_writer: Optional[BulkWriter] = None,
        **pymongo_kwargs: Any,
    ):
        super().__init__()
        self.set_session(session)
        self.document_model = document_model
        self.find_query = find_query
        self.bulk_writer = bulk_writer
        self.pymongo_kwargs: Dict[str, Any] = pymongo_kwargs


class DeleteMany(DeleteQuery):
    def __await__(self) -> Generator[None, None, Optional[DeleteResult]]:
        if self.bulk_writer is not None:
            return self.bulk_writer.add_operation(
                Operation(
                    operation=DeleteManyPyMongo,
                    first_query=self.find_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
        return (
            yield from self.document_model.get_motor_collection()
            .delete_many(
                self.find_query, session=self.session, **self.pymongo_kwargs
            )
            .__await__()
        )


class DeleteOne(DeleteQuery):
    def __await__(self) -> Generator[None, None, Optional[DeleteResult]]:
        if self.bulk_writer is not None:
            return self.bulk_writer.add_operation(
                Operation(
                    operation=DeleteOnePyMongo,
                    first_query=self.find_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
        return (
            yield from self.document_model.get_motor_collection()
            .delete_one(
                self.find_query, session=self.session, **self.pymongo_kwargs
            )
            .__await__()
        )
