from typing import Any, Type, Union

from pydantic import BaseModel

import beanie
from beanie.exceptions import (
    DocWasNotRegisteredInUnionClass,
    UnionHasNoRegisteredDocs,
)


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
