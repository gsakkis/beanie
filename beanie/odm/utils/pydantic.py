from typing import Any, Type

from pydantic import BaseModel, TypeAdapter


def parse_object_as(object_type: Type, data: Any):
    return TypeAdapter(object_type).validate_python(data)


def get_field_type(field):
    return field.annotation


def get_model_fields(model):
    return model.model_fields


def parse_model(model_type: Type[BaseModel], data: Any):
    return model_type.model_validate(data)


def get_extra_field_info(field, parameter: str):
    if field.json_schema_extra is not None:
        return field.json_schema_extra.get(parameter)
    return None


def get_config_value(model, parameter: str):
    return model.model_config.get(parameter)


def get_model_dump(model):
    return model.model_dump()
