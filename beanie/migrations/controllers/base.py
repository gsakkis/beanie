from abc import ABC, abstractmethod
from typing import Sequence, Type

from pymongo.client_session import ClientSession

from beanie.odm.documents import Document


class BaseMigrationController(ABC):
    @abstractmethod
    async def run(self, session: ClientSession) -> None:
        pass

    @property
    @abstractmethod
    def models(self) -> Sequence[Type[Document]]:
        pass
