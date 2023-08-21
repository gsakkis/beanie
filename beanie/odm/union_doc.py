from typing import ClassVar, Dict, Optional, Type

from beanie.exceptions import UnionDocNotInited
from beanie.odm.interfaces.find import FindInterface
from beanie.odm.settings.union_doc import UnionDocSettings


class UnionDoc(FindInterface):
    _sort_order: ClassVar[int] = 0
    _document_models: ClassVar[Optional[Dict[str, Type]]] = None
    _is_inited: ClassVar[bool] = False
    _settings: ClassVar[UnionDocSettings]

    @classmethod
    def get_settings(cls) -> UnionDocSettings:
        return cls._settings

    @classmethod
    def register_doc(cls, name: str, doc_model: Type):
        if cls._document_models is None:
            cls._document_models = {}

        if cls._is_inited is False:
            raise UnionDocNotInited

        cls._document_models[name] = doc_model
        return cls.get_settings().name
