from typing import Any, Dict, List, Type

from beanie.odm.interfaces.find import FindInterface
from beanie.odm.settings.base import ItemSettings


class ViewSettings(ItemSettings):
    source: Type[FindInterface]
    pipeline: List[Dict[str, Any]]
