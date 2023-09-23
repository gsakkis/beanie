import inspect
from typing import TYPE_CHECKING, Union, get_args

if TYPE_CHECKING:
    from types import GenericAlias


def extract_id_class(annotation: Union[type, "GenericAlias"]) -> type:
    if inspect.isclass(annotation):
        return annotation

    try:
        first_arg = next(
            arg for arg in get_args(annotation) if arg is not type(None)
        )
    except StopIteration:
        raise ValueError("Unknown annotation: {}".format(annotation))
    return extract_id_class(first_arg)
