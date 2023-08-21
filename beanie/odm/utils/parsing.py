from typing import Any, Type, Union

from pydantic import BaseModel

import beanie
from beanie.exceptions import (
    DocWasNotRegisteredInUnionClass,
    UnionHasNoRegisteredDocs,
)


def merge_models(left: BaseModel, right: BaseModel) -> None:
    from beanie.odm.fields import Link

    if hasattr(left, "_previous_revision_id") and hasattr(
        right, "_previous_revision_id"
    ):
        left._previous_revision_id = right._previous_revision_id  # type: ignore
    for k, right_value in right.__iter__():
        left_value = getattr(left, k)
        if isinstance(right_value, BaseModel) and isinstance(
            left_value, BaseModel
        ):
            merge_models(left_value, right_value)
            continue
        if isinstance(right_value, list):
            links_found = False
            for i in right_value:
                if isinstance(i, Link):
                    links_found = True
                    break
            if links_found:
                continue
        elif not isinstance(right_value, Link):
            left.__setattr__(k, right_value)


def parse_obj(
    model: Union[Type[BaseModel], Type["beanie.UnionDoc"]],
    data: Any,
    lazy_parse: bool = False,
) -> BaseModel:
    if issubclass(model, beanie.UnionDoc):
        if model._document_models is None:
            raise UnionHasNoRegisteredDocs

        if isinstance(data, dict):
            class_name = data[model.get_settings().class_id]
        else:
            class_name = data._class_id

        if class_name not in model._document_models:
            raise DocWasNotRegisteredInUnionClass
        return parse_obj(model._document_models[class_name], data, lazy_parse)

    if issubclass(model, beanie.Document) and model._inheritance_inited:
        if isinstance(data, dict):
            class_name = data.get(model.get_settings().class_id)
        elif hasattr(data, model.get_settings().class_id):
            class_name = data._class_id
        else:
            class_name = None

        if model._children and class_name in model._children:
            return parse_obj(model._children[class_name], data, lazy_parse)

    if issubclass(model, beanie.Document) and lazy_parse:
        o = model.lazy_parse(data, {"_id"})
        o._saved_state = {"_id": o.id}
        return o

    result = model.model_validate(data)

    if isinstance(result, beanie.Document):
        result._save_state()
        result._swap_revision()

    return result
