from dataclasses import dataclass, field
from typing import Any, Generator, Mapping, Optional

import pymongo
from pymongo.results import DeleteResult

from beanie.odm.bulk import BulkWriter, Operation
from beanie.odm.queries import BaseQuery


@dataclass
class DeleteQuery(BaseQuery):
    """Deletion Query"""

    find_query: Mapping[str, Any] = field(default_factory=dict)
    bulk_writer: Optional[BulkWriter] = None

    async def _delete(self, many: bool) -> Optional[DeleteResult]:
        if self.bulk_writer is not None:
            return self.bulk_writer.add_operation(
                Operation(
                    operation_class=(
                        pymongo.DeleteMany if many else pymongo.DeleteOne
                    ),
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
