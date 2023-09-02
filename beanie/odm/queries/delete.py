from typing import TYPE_CHECKING, Any, Generator, Mapping, Optional, Type

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
        bulk_writer: Optional[BulkWriter] = None,
        session: Optional[ClientSession] = None,
        **pymongo_kwargs: Any,
    ):
        super().__init__(session, **pymongo_kwargs)
        self.document_model = document_model
        self.find_query = find_query
        self.bulk_writer = bulk_writer

    async def _delete(self, many: bool) -> Optional[DeleteResult]:
        if self.bulk_writer is not None:
            return self.bulk_writer.add_operation(
                Operation(
                    operation=DeleteManyPyMongo if many else DeleteOnePyMongo,
                    first_query=self.find_query,
                    object_class=self.document_model,
                    pymongo_kwargs=self.pymongo_kwargs,
                )
            )
        collection = self.document_model.get_motor_collection()
        method = collection.delete_many if many else collection.delete_one
        return await method(
            self.find_query, session=self.session, **self.pymongo_kwargs
        )


class DeleteMany(DeleteQuery):
    def __await__(self) -> Generator[None, None, Optional[DeleteResult]]:
        return self._delete(many=True).__await__()


class DeleteOne(DeleteQuery):
    def __await__(self) -> Generator[None, None, Optional[DeleteResult]]:
        return self._delete(many=False).__await__()
