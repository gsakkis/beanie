from typing import Any, Dict, Type, Union

from pydantic import BaseModel

import beanie


def merge_models(left: BaseModel, right: BaseModel) -> None:
    if isinstance(left, beanie.Document) and isinstance(
        right, beanie.Document
    ):
        left._previous_revision_id = right._previous_revision_id
    for k, rvalue in right:
        lvalue = getattr(left, k)
        if isinstance(rvalue, BaseModel) and isinstance(lvalue, BaseModel):
            merge_models(lvalue, rvalue)
        elif not isinstance(rvalue, (beanie.Link, list)):
            setattr(left, k, rvalue)


ParseableModel = Union[BaseModel, "beanie.UnionDoc"]


def parse_obj(
    model: Type[ParseableModel],
    data: Union[BaseModel, Dict[str, Any]],
    lazy_parse: bool = False,
) -> BaseModel:
    if isinstance(data, BaseModel):
        if type(data) is model:
            return data
        raise TypeError(
            f"Cannot parse {data} of type {type(data)} as {model}. "
            f"Only dict or {model} is allowed."
        )

    if issubclass(model, beanie.UnionDoc):
        class_name = data[model.get_settings().class_id]
        return parse_obj(model._children[class_name], data, lazy_parse)

    if issubclass(model, beanie.Document) and model._children:
        class_name = data[model.get_settings().class_id]
        if class_name in model._children:
            return parse_obj(model._children[class_name], data, lazy_parse)

    if issubclass(model, beanie.Document) and lazy_parse:
        o = model.lazy_parse(data, {"_id"})
        o._saved_state = {"_id": o.id}
        return o

    result = model.model_validate(data)

    if isinstance(result, beanie.Document):
        result.save_state()
        result.swap_revision()

    return result
