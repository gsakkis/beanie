from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Type, Union

import pymongo
from pymongo.client_session import ClientSession
from pymongo.results import BulkWriteResult
from typing_extensions import Self

from beanie.odm.interfaces.settings import BaseSettings, SettingsInterface

PyMongoOperation = Union[
    pymongo.InsertOne,
    pymongo.DeleteOne,
    pymongo.DeleteMany,
    pymongo.ReplaceOne,
    pymongo.UpdateOne,
    pymongo.UpdateMany,
]


@dataclass
class Operation:
    operation_class: Type[PyMongoOperation]
    object_class: Type[SettingsInterface[BaseSettings]]
    first_query: Mapping[str, Any]
    second_query: Union[
        Mapping[str, Any], List[Mapping[str, Any]], None
    ] = None
    pymongo_kwargs: Dict[str, Any] = field(default_factory=dict)


class BulkWriter:
    def __init__(self, session: Optional[ClientSession] = None):
        self.operations: List[Operation] = []
        self.session = session

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.commit()

    async def commit(self) -> Optional[BulkWriteResult]:
        """
        Commit all the operations to the database
        :return:
        """
        if not self.operations:
            return None

        requests = []
        obj_class = self.operations[0].object_class
        for op in self.operations:
            if obj_class != op.object_class:
                raise ValueError(
                    "All the operations should be for a single document model"
                )
            request: PyMongoOperation
            op_class = op.operation_class
            if (
                issubclass(op_class, pymongo.InsertOne)
                or issubclass(op_class, pymongo.DeleteOne)
                or issubclass(op_class, pymongo.DeleteMany)
            ):
                assert op.second_query is None
                request = op_class(op.first_query, **op.pymongo_kwargs)
            else:
                assert op.second_query is not None
                request = op_class(
                    op.first_query, op.second_query, **op.pymongo_kwargs
                )
            requests.append(request)

        return await obj_class.get_motor_collection().bulk_write(
            requests, session=self.session
        )

    def add_operation(self, operation: Operation) -> None:
        self.operations.append(operation)
