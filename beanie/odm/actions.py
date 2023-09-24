import asyncio
import inspect
from collections import defaultdict
from enum import Enum
from functools import wraps
from typing import (
    Awaitable,
    Callable,
    ClassVar,
    Container,
    Dict,
    List,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import ParamSpec, TypeAlias

import beanie


class EventTypes(str, Enum):
    INSERT = "INSERT"
    REPLACE = "REPLACE"
    SAVE = "SAVE"
    SAVE_CHANGES = "SAVE_CHANGES"
    VALIDATE_ON_SAVE = "VALIDATE_ON_SAVE"
    DELETE = "DELETE"
    UPDATE = "UPDATE"


class ActionDirections(str, Enum):  # TODO think about this name
    BEFORE = "BEFORE"
    AFTER = "AFTER"


P = ParamSpec("P")
R = TypeVar("R")
AsyncFunc: TypeAlias = Callable[P, Awaitable[R]]
Document: TypeAlias = "beanie.Document"
Action: TypeAlias = Callable[[Document], None]


class ActionRegistry:
    _actions: ClassVar[
        Dict[Action, Tuple[List[EventTypes], ActionDirections]]
    ] = {}
    _type_actions: ClassVar[
        Dict[Tuple[Type[Document], EventTypes, ActionDirections], List[Action]]
    ] = defaultdict(list)

    @classmethod
    def register_action(
        cls,
        action_direction: ActionDirections,
        *event_types: Union[List[EventTypes], EventTypes],
    ) -> Callable[[Action], Action]:
        flat_event_types = []
        for event_type in event_types:
            if isinstance(event_type, EventTypes):
                flat_event_types.append(event_type)
            else:
                flat_event_types.extend(event_type)

        def decorator(f: Action) -> Action:
            cls._actions[f] = (flat_event_types, action_direction)
            return f

        return decorator

    @classmethod
    def register_type(cls, doc_type: Type[Document]) -> None:
        for key in list(cls._type_actions.keys()):
            if key[0] == doc_type:
                del cls._type_actions[key]

        actions = set(cls._actions.keys())
        actions.intersection_update(
            f for _, f, in inspect.getmembers(doc_type, inspect.isfunction)
        )
        for action in actions:
            event_types, action_direction = cls._actions[action]
            for event_type in event_types:
                key = doc_type, event_type, action_direction
                cls._type_actions[key].append(action)

    @classmethod
    def wrap_with_actions(
        cls, event_type: EventTypes
    ) -> Callable[[AsyncFunc[P, R]], AsyncFunc[P, R]]:
        def decorator(f: AsyncFunc[P, R]) -> AsyncFunc[P, R]:
            @wraps(f)
            async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                if not args or not isinstance(args[0], beanie.Document):
                    raise TypeError(
                        "First argument should be a Document instance"
                    )
                skip_actions = cast(
                    Container[Union[ActionDirections, str]],
                    kwargs.get("skip_actions") or (),
                )
                await cls._run_actions(
                    args[0], event_type, ActionDirections.BEFORE, skip_actions
                )
                result = await f(*args, **kwargs)
                await cls._run_actions(
                    args[0], event_type, ActionDirections.AFTER, skip_actions
                )
                return result

            return wrapper

        return decorator

    @classmethod
    async def _run_actions(
        cls,
        document: Document,
        event_type: EventTypes,
        action_direction: ActionDirections,
        skip_actions: Container[Union[ActionDirections, str]],
    ) -> None:
        if action_direction in skip_actions:
            return
        coros = []
        key = document.__class__, event_type, action_direction
        for action in cls._type_actions.get(key, ()):
            if action.__name__ in skip_actions:
                continue
            if inspect.iscoroutinefunction(action):
                coros.append(action(document))
            elif inspect.isfunction(action):
                action(document)
        if coros:
            await asyncio.gather(*coros)


def before_event(
    *event_types: Union[List[EventTypes], EventTypes]
) -> Callable[[Action], Action]:
    """
    Decorator. It adds action, which should run before mentioned one
    or many events happen

    :param event_types: Union[List[EventTypes], EventTypes] - event types
    """
    return ActionRegistry.register_action(
        ActionDirections.BEFORE, *event_types
    )


def after_event(
    *event_types: Union[List[EventTypes], EventTypes]
) -> Callable[[Action], Action]:
    """
    Decorator. It adds action, which should run after mentioned one
    or many events happen

    :param event_types: Union[List[EventTypes], EventTypes] - event types
    """
    return ActionRegistry.register_action(ActionDirections.AFTER, *event_types)
