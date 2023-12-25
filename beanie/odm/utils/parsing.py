from typing import Any, Mapping, Type, Union

from pydantic import BaseModel

import beanie


def merge_models(left: BaseModel, right: BaseModel) -> None:
    for k, rvalue in right:
        lvalue = getattr(left, k)
        if (
            isinstance(rvalue, BaseModel)
            and isinstance(lvalue, BaseModel)
            and not lvalue.model_config.get("frozen")
        ):
            merge_models(lvalue, rvalue)
        elif isinstance(rvalue, beanie.Link) or (
            isinstance(rvalue, list)
            and any(isinstance(item, beanie.Link) for item in rvalue)
        ):
            pass
        else:
            setattr(left, k, rvalue)


ParseableModel = Union[BaseModel, "beanie.UnionDoc"]


def parse_obj(
    model: Type[ParseableModel],
    data: Union[BaseModel, Mapping[str, Any]],
    lazy_parse: bool = False,
) -> BaseModel:
    if isinstance(data, Mapping):
        if issubclass(model, beanie.UnionDoc):
            class_name = data[model.get_settings().class_id]
            return parse_obj(model._children[class_name], data, lazy_parse)

        if issubclass(model, beanie.Document) and model._children:
            class_name = data[model.get_settings().class_id]
            if class_name in model._children:
                return parse_obj(model._children[class_name], data, lazy_parse)

        if issubclass(model, beanie.Document) and lazy_parse:
            return model.lazy_parse(data)

        result = model.model_validate(data)
    elif type(data) is model:
        result = data
    else:
        raise TypeError(
            f"Cannot parse {data} of type {type(data)} as {model}. "
            f"Only dict or {model} is allowed."
        )

    if isinstance(result, beanie.Document):
        result._save_state()

    return result
