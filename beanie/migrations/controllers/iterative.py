import asyncio
from functools import partial
from inspect import signature
from typing import Any, Callable, Dict, Mapping, Sequence, Type, Union

from pydantic.utils import lenient_issubclass
from pymongo.client_session import ClientSession

from beanie.migrations.controllers.base import BaseMigrationController
from beanie.migrations.controllers.free_fall import (
    MigrationFunction,
    drop_self,
)
from beanie.odm.documents import Document


def recursive_update(d: Dict[str, Any], u: Mapping[str, Any]) -> None:
    for k, v in u.items():
        if isinstance(v, dict):
            d.setdefault(k, {})
            recursive_update(d[k], v)
        else:
            d[k] = v


class DummyDocument(Dict[str, Any]):
    def __setattr__(self, key: str, value: Any) -> None:
        self[key] = value

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError:
            return self.setdefault(attr, DummyDocument())

    def dict(
        self, input_dict: Union[Dict[str, Any], None] = None
    ) -> Dict[str, Any]:
        if input_dict is None:
            input_dict = self
        return {
            k: self.dict(v) if isinstance(v, dict) else v
            for k, v in input_dict.items()
        }


class IterativeMigration(BaseMigrationController):
    def __init__(
        self,
        document_models: Sequence[Type[Document]],
        batch_size: int,
        function: MigrationFunction,
    ):
        function_params = signature(function).parameters
        self.input_document_model = self._validate_parameter(
            function_params, "input_document"
        )
        self.output_document_model = self._validate_parameter(
            function_params, "output_document"
        )
        self.function = drop_self(function)
        self.document_models = document_models
        self.batch_size = batch_size

    @property
    def models(self) -> Sequence[Type[Document]]:
        return [
            *self.document_models,
            self.input_document_model,
            self.output_document_model,
        ]

    async def run(self, session: ClientSession) -> None:
        replacements = []
        output_documents = []
        input_documents = self.input_document_model.find_all(session=session)
        async for input_document in input_documents:
            output_document = DummyDocument()
            await self.function(
                input_document=input_document, output_document=output_document
            )
            output_dict = input_document.dict()
            recursive_update(output_dict, output_document.dict())
            output_documents.append(
                self.output_document_model.model_validate(output_dict)
            )
            if len(output_documents) == self.batch_size:
                replacements.append(
                    self.output_document_model.replace_many(
                        output_documents, session=session
                    )
                )
                output_documents = []
        if output_documents:
            replacements.append(
                self.output_document_model.replace_many(
                    output_documents, session=session
                )
            )
        await asyncio.gather(*replacements)

    @staticmethod
    def _validate_parameter(
        function_params: Mapping[str, Any], name: str
    ) -> Type[Document]:
        signature = function_params.get(name)
        if signature is None:
            raise TypeError(f"function must take {name} parameter")
        annotation: Type[Document] = signature.annotation
        if not lenient_issubclass(annotation, Document):
            raise TypeError(
                f"{name} must have annotation of Document subclass"
            )
        return annotation


def iterative_migration(
    document_models: Sequence[Type[Document]] = (), batch_size: int = 10000
) -> Callable[[MigrationFunction], BaseMigrationController]:
    return partial(IterativeMigration, document_models, batch_size)
