import inspect
from typing import get_args


def extract_id_class(annotation) -> type:
    if inspect.isclass(annotation):
        return annotation

    try:
        first_arg = next(
            arg for arg in get_args(annotation) if arg is not type(None)
        )
    except StopIteration:
        raise ValueError("Unknown annotation: {}".format(annotation))
    return extract_id_class(first_arg)
