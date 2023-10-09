import logging
from dataclasses import dataclass
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import Optional, Type

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from beanie.odm.documents import Document
from beanie.odm.utils.init import init_beanie

from .controllers.base import BaseMigrationController
from .models import MigrationLog, RunningDirections, RunningMode

logger = logging.getLogger(__name__)


@dataclass
class MigrationNode:
    """
    Node of the migration linked list

    :param client: AsyncIOMotorClient
    :param database: AsyncIOMotorDatabase
    :param name: name of the migration
    :param forward_class: Forward class of the migration
    :param backward_class: Backward class of the migration
    :param next_migration: link to the next migration
    :param prev_migration: link to the previous migration
    """

    client: AsyncIOMotorClient
    database: AsyncIOMotorDatabase
    name: str
    forward_class: Optional[Type[Document]] = None
    backward_class: Optional[Type[Document]] = None
    next_migration: Optional["MigrationNode"] = None
    prev_migration: Optional["MigrationNode"] = None

    @staticmethod
    async def clean_current_migration() -> None:
        await MigrationLog.find_many({"is_current": True}).update(
            {"$set": {"is_current": False}}
        )

    async def update_current_migration(self) -> None:
        await self.clean_current_migration()
        await MigrationLog(is_current=True, name=self.name).insert()

    async def run(self, mode: RunningMode, allow_index_dropping: bool) -> None:
        if mode.direction == RunningDirections.FORWARD:
            migration_node = self.next_migration
            if migration_node is None:
                return None
            if mode.distance == 0:
                logger.info("Running migrations forward without limit")
                while True:
                    await migration_node.run_forward(allow_index_dropping)
                    migration_node = migration_node.next_migration
                    if migration_node is None:
                        break
            else:
                logger.info(f"Running {mode.distance} migrations forward")
                for i in range(mode.distance):
                    await migration_node.run_forward(allow_index_dropping)
                    migration_node = migration_node.next_migration
                    if migration_node is None:
                        break
        elif mode.direction == RunningDirections.BACKWARD:
            migration_node = self
            if mode.distance == 0:
                logger.info("Running migrations backward without limit")
                while True:
                    await migration_node.run_backward(allow_index_dropping)
                    migration_node = migration_node.prev_migration
                    if migration_node is None:
                        break
            else:
                logger.info(f"Running {mode.distance} migrations backward")
                for i in range(mode.distance):
                    await migration_node.run_backward(allow_index_dropping)
                    migration_node = migration_node.prev_migration
                    if migration_node is None:
                        break

    async def run_forward(self, allow_index_dropping: bool) -> None:
        if self.forward_class is not None:
            await self.run_migration_class(
                self.forward_class, allow_index_dropping=allow_index_dropping
            )
        await self.update_current_migration()

    async def run_backward(self, allow_index_dropping: bool) -> None:
        if self.backward_class is not None:
            await self.run_migration_class(
                self.backward_class, allow_index_dropping=allow_index_dropping
            )
        if self.prev_migration is not None:
            await self.prev_migration.update_current_migration()
        else:
            await self.clean_current_migration()

    async def run_migration_class(
        self, cls: Type[Document], allow_index_dropping: bool
    ) -> None:
        migrations = [
            getattr(cls, migration)
            for migration in dir(cls)
            if isinstance(getattr(cls, migration), BaseMigrationController)
        ]

        async with await self.client.start_session() as s:
            async with s.start_transaction():
                for migration in migrations:
                    for model in migration.models:
                        await init_beanie(
                            database=self.database,
                            document_models=[model],
                            allow_index_dropping=allow_index_dropping,
                        )  # TODO this is slow
                    logger.info(
                        f"Running migration {migration.function.__name__} "
                        f"from module {self.name}"
                    )
                    await migration.run(session=s)

    @classmethod
    async def build(
        cls, path: Path, db_uri: str, db_name: str
    ) -> "MigrationNode":
        logger.info("Building migration list")
        names = sorted(modulepath.name for modulepath in path.glob("*.py"))

        client = AsyncIOMotorClient(db_uri)
        database = client[db_name]
        await init_beanie(database=database, document_models=[MigrationLog])
        current_migration = await MigrationLog.find_one({"is_current": True})
        root_migration_node = cls(client, database, "root")
        prev_migration_node = root_migration_node
        for name in names:
            file_path = path / name
            loader = SourceFileLoader(
                file_path.stem, str(file_path.absolute())
            )
            module = loader.load_module(file_path.stem)
            migration_node = cls(
                client=client,
                database=database,
                name=name,
                prev_migration=prev_migration_node,
                forward_class=getattr(module, "Forward", None),
                backward_class=getattr(module, "Backward", None),
            )
            prev_migration_node.next_migration = migration_node
            prev_migration_node = migration_node
            if (
                current_migration is not None
                and current_migration.name == name
            ):
                root_migration_node = migration_node

        return root_migration_node
