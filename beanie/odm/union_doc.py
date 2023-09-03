from typing import ClassVar, Dict, Optional, Type

from beanie.odm.interfaces.find import FindInterface


class UnionDoc(FindInterface):
    _sort_order: ClassVar[int] = 0
    _document_models: ClassVar[Optional[Dict[str, Type]]] = None

    @classmethod
    def register_doc(cls, name: str, doc_model: Type):
        if cls._document_models is None:
            cls._document_models = {}

        cls._document_models[name] = doc_model
        return cls._settings.name
