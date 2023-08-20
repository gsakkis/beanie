from typing import Dict, ForwardRef, Generic, Type, TypeVar, Union

T = TypeVar("T")


class DocsRegistry(Generic[T]):
    def __init__(self):
        self._registry: Dict[str, Type[T]] = {}

    def register(self, name: str, doc_type: Type[T]):
        self._registry[name] = doc_type

    def get(self, name: str) -> Type[T]:
        return self._registry[name]

    def evaluate_fr(self, forward_ref: Union[ForwardRef, Type]):
        """
        Evaluate forward ref

        :param forward_ref: ForwardRef - forward ref to evaluate
        :return: Type[BaseModel] - class of the forward ref
        """
        if (
            isinstance(forward_ref, ForwardRef)
            and forward_ref.__forward_arg__ in self._registry
        ):
            return self._registry[forward_ref.__forward_arg__]
        else:
            return forward_ref
