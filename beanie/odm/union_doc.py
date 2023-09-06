from typing import ClassVar, Dict, Type

from beanie.odm.interfaces.find import FindInterface
from beanie.odm.settings.base import ItemSettings


class UnionDocSettings(ItemSettings):
    ...


class UnionDoc(FindInterface):
    _children: ClassVar[Dict[str, Type]]
