from typing import Any, Dict, Optional

from pydantic import BaseModel

import beanie


def get_projection(model: type) -> Optional[Dict[str, Any]]:
    if not issubclass(model, BaseModel):
        return None

    if issubclass(model, beanie.Document) and model._inheritance_inited:
        return None

    if hasattr(model, "Settings"):  # MyPy checks
        settings = getattr(model, "Settings")
        if hasattr(settings, "projection"):
            return getattr(settings, "projection")

    if model.model_config.get("extra") == "allow":
        return None

    return {
        field.alias or name: 1 for name, field in model.model_fields.items()
    }
