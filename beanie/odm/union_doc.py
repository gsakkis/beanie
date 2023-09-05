from typing import ClassVar, Dict, Type

from beanie.odm.interfaces.find import FindInterface


class UnionDoc(FindInterface):
    _sort_order: ClassVar[int] = 0
    _children: ClassVar[Dict[str, Type]]
