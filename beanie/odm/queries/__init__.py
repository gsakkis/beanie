from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Type

from pymongo.client_session import ClientSession
from typing_extensions import Self

from beanie.odm.interfaces.settings import SettingsInterface
from beanie.odm.utils.encoder import Encoder


@dataclass
class BaseQuery:
    """Base class of all queries"""

    document_model: Type[SettingsInterface]
    session: Optional[ClientSession] = None
    pymongo_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        bson_encoders = self.document_model.get_settings().bson_encoders
        self.encoder = Encoder(custom_encoders=bson_encoders)

    def set_session(self, session: Optional[ClientSession] = None) -> Self:
        """
        Set pymongo session
        :param session: Optional[ClientSession] - pymongo session
        :return: self
        """
        if session is not None:
            self.session = session
        return self


@dataclass
class CacheableQuery(ABC, BaseQuery):
    ignore_cache: bool = False

    @property
    def _cache_key(self) -> str:
        return str(self._cache_key_dict())

    @abstractmethod
    def _cache_key_dict(self) -> Dict[str, Any]:
        ...
