from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Type, Union

from pymongo import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    UpdateMany,
    UpdateOne,
)
from pymongo.client_session import ClientSession
from pymongo.results import BulkWriteResult


@dataclass
class Operation:
    operation: Union[
        Type[InsertOne],
        Type[DeleteOne],
        Type[DeleteMany],
        Type[ReplaceOne],
        Type[UpdateOne],
        Type[UpdateMany],
    ]
    object_class: Type
    first_query: Mapping[str, Any]
    second_query: Optional[Dict[str, Any]] = None
    pymongo_kwargs: Dict[str, Any] = field(default_factory=dict)


class BulkWriter:
    def __init__(self, session: Optional[ClientSession] = None):
        self.operations: List[Operation] = []
        self.session = session

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.commit()

    async def commit(self) -> Optional[BulkWriteResult]:
        """
        Commit all the operations to the database
        :return:
        """
        requests = []
        if self.operations:
            obj_class = self.operations[0].object_class
            for op in self.operations:
                if obj_class != op.object_class:
                    raise ValueError(
                        "All the operations should be for a single document model"
                    )

                args = [op.first_query]
                if op.second_query is not None:
                    args.append(op.second_query)
                requests.append(op.operation(*args, **op.pymongo_kwargs))

            return await obj_class.get_motor_collection().bulk_write(
                requests, session=self.session
            )
        return None

    def add_operation(self, operation: Operation):
        self.operations.append(operation)
