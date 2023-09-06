from typing import ClassVar, Dict, Type

from beanie.odm.interfaces.find import FindInterface
from beanie.odm.settings import BaseSettings


class UnionDocSettings(BaseSettings):
    ...


class UnionDoc(FindInterface):
    _children: ClassVar[Dict[str, Type]]
