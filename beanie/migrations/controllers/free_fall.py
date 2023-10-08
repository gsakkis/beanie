from functools import partial, wraps
from inspect import signature
from typing import Awaitable, Callable, Sequence, Type

from pymongo.client_session import ClientSession

from beanie.migrations.controllers.base import BaseMigrationController
from beanie.odm.documents import Document

MigrationFunction = Callable[..., Awaitable[None]]


def drop_self(function: MigrationFunction) -> MigrationFunction:
    if "self" in signature(function).parameters:
        function = wraps(function)(partial(function, None))
    return function


class FreeFallMigrationController(BaseMigrationController):
    def __init__(
        self,
        document_models: Sequence[Type[Document]],
        function: MigrationFunction,
    ):
        self.function = drop_self(function)
        self.document_models = document_models

    @property
    def models(self) -> Sequence[Type[Document]]:
        return self.document_models

    async def run(self, session: ClientSession) -> None:
        await self.function(session=session)


def free_fall_migration(
    document_models: Sequence[Type[Document]],
) -> Callable[[MigrationFunction], BaseMigrationController]:
    return partial(FreeFallMigrationController, document_models)
