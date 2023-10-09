import asyncio
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import click
import toml

from beanie.migrations import template
from beanie.migrations.models import RunningDirections, RunningMode
from beanie.migrations.runner import MigrationNode

logging.basicConfig(format="%(message)s", level=logging.INFO)


class MigrationSettings:
    def __init__(
        self,
        direction: Optional[str] = None,
        distance: Optional[int] = None,
        connection_uri: Optional[str] = None,
        database_name: Optional[str] = None,
        path: Optional[str] = None,
        allow_index_dropping: bool = False,
    ):
        self.direction = RunningDirections(
            direction
            or self.get_env_value("direction")
            or self.get_from_toml("direction")
            or "FORWARD"
        )
        self.distance = int(
            distance
            or self.get_env_value("distance")
            or self.get_from_toml("distance")
            or 0
        )
        self.connection_uri = str(
            connection_uri
            or self.get_env_value("connection_uri")
            or self.get_from_toml("connection_uri")
        )
        self.database_name = str(
            database_name
            or self.get_env_value("database_name")
            or self.get_from_toml("database_name")
        )
        self.path = Path(
            path or self.get_env_value("path") or self.get_from_toml("path")
        )
        self.allow_index_dropping = bool(
            allow_index_dropping
            or self.get_env_value("allow_index_dropping")
            or self.get_from_toml("allow_index_dropping")
            or False
        )

    @staticmethod
    def get_env_value(field_name: str) -> Optional[str]:
        def get_value(key: str) -> Optional[str]:
            return os.getenv(key.upper()) or os.getenv(key.lower())

        if field_name == "connection_uri":
            return (
                get_value("BEANIE_URI")
                or get_value("BEANIE_CONNECTION_URI")
                or get_value("BEANIE_CONNECTION_STRING")
                or get_value("BEANIE_MONGODB_DSN")
                or get_value("BEANIE_MONGODB_URI")
            )
        if field_name == "database_name":
            return (
                get_value("BEANIE_DB")
                or get_value("BEANIE_DB_NAME")
                or get_value("BEANIE_DATABASE_NAME")
            )
        return get_value(f"BEANIE_{field_name}")

    @staticmethod
    def get_from_toml(field_name: str) -> Any:
        path = Path("pyproject.toml")
        if path.is_file():
            val = (
                toml.load(path)
                .get("tool", {})
                .get("beanie", {})
                .get("migrations", {})
            )
        else:
            val = {}
        return val.get(field_name)


async def run_migrate(settings: MigrationSettings) -> None:
    root = await MigrationNode.build(
        settings.path, settings.connection_uri, settings.database_name
    )
    mode = RunningMode(
        direction=settings.direction, distance=settings.distance
    )
    await root.run(
        mode=mode, allow_index_dropping=settings.allow_index_dropping
    )


@click.group()
def migrations() -> None:
    pass


@migrations.command()
@click.option(
    "--forward",
    "direction",
    flag_value="FORWARD",
    help="Roll the migrations forward. This is default",
)
@click.option(
    "--backward",
    "direction",
    flag_value="BACKWARD",
    help="Roll the migrations backward",
)
@click.option(
    "-d",
    "--distance",
    type=int,
    help="How many migrations should be done since the current? 0 - all the migrations. Default is 0",
)
@click.option("-uri", "--connection-uri", help="MongoDB connection URI")
@click.option("-db", "--database_name", help="DataBase name")
@click.option("-p", "--path", help="Path to the migrations directory")
@click.option(
    "--allow-index-dropping/--forbid-index-dropping",
    default=False,
    help="if allow-index-dropping is set, Beanie will drop indexes from your collection",
)
def migrate(
    direction: Optional[str],
    distance: Optional[int],
    connection_uri: Optional[str],
    database_name: Optional[str],
    path: Optional[str],
    allow_index_dropping: bool,
) -> None:
    settings = MigrationSettings(
        direction=direction,
        distance=distance,
        connection_uri=connection_uri,
        database_name=database_name,
        path=path,
        allow_index_dropping=allow_index_dropping,
    )
    asyncio.run(run_migrate(settings))


@migrations.command()
@click.option("-n", "--name", required=True, help="Migration name")
@click.option(
    "-p",
    "--path",
    required=True,
    type=Path,
    help="Path to the migrations directory",
)
def new_migration(name: str, path: Path) -> None:
    ts_string = datetime.now().strftime("%Y%m%d%H%M%S")
    file_name = f"{ts_string}_{name}.py"
    shutil.copy(template.__file__, path / file_name)


if __name__ == "__main__":
    migrations()
