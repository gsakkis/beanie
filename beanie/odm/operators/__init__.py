from typing import TYPE_CHECKING, Any, ClassVar, Iterator, Mapping

if TYPE_CHECKING:
    from beanie.odm.fields import FieldExpr


class BaseOperator(Mapping[str, Any]):
    def __init__(self, key: str, value: Any):
        assert isinstance(key, str)
        self._key = key
        self._value = value

    def __getitem__(self, key: str) -> Any:
        if key == self._key:
            return self._value
        raise KeyError(key)

    def __iter__(self) -> Iterator[str]:
        yield self._key

    def __len__(self) -> int:
        return 1


class BaseNonFieldOperator(BaseOperator):
    operator: ClassVar[str]

    def __init__(self, expression: Any):
        super().__init__(self.operator, expression)


class BaseFieldOperator(BaseOperator):
    operator: ClassVar[str]

    def __init__(self, field: "FieldExpr", expression: Any):
        super().__init__(str(field), {self.operator: expression})
