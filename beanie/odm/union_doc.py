from typing import ClassVar, Dict, Type

from beanie.odm.interfaces.find import FindInterface


class UnionDoc(FindInterface):
    _children: ClassVar[Dict[str, Type]]
