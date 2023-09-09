import asyncio
import inspect
from collections import defaultdict
from enum import Enum
from functools import wraps
from typing import Any, Callable, Dict, List, Tuple, Union


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


class ActionRegistry:
    _actions: Dict[
        Callable[..., Any],
        Tuple[List[EventTypes], ActionDirections],
    ] = {}
    _type_actions: Dict[
        Tuple[type, EventTypes, ActionDirections],
        List[Callable[..., Any]],
    ] = defaultdict(list)

    @classmethod
    def register(
        cls,
        action_direction: ActionDirections,
        *event_types: Union[List[EventTypes], EventTypes],
    ):
        flat_event_types = []
        for event_type in event_types:
            if isinstance(event_type, EventTypes):
                flat_event_types.append(event_type)
            else:
                flat_event_types.extend(event_type)

        def decorator(f):
            cls._actions[f] = (flat_event_types, action_direction)
            return f

        return decorator

    @classmethod
    def init_actions(cls, doc_type: type) -> None:
        for key in list(cls._type_actions.keys()):
            if key[0] == doc_type:
                del cls._type_actions[key]

        actions = set(cls._actions)
        actions.intersection_update(
            f for _, f, in inspect.getmembers(doc_type, inspect.isfunction)
        )
        for action in actions:
            event_types, action_direction = cls._actions[action]
            for event_type in event_types:
                key = doc_type, event_type, action_direction
                cls._type_actions[key].append(action)

    @classmethod
    async def run_actions(
        cls,
        instance: Any,
        event_type: EventTypes,
        action_direction: ActionDirections,
        skip_actions: List[Union[ActionDirections, str]],
    ):
        if action_direction in skip_actions:
            return
        coros = []
        key = instance.__class__, event_type, action_direction
        for action in cls._type_actions.get(key, ()):
            if action.__name__ in skip_actions:
                continue
            if inspect.iscoroutinefunction(action):
                coros.append(action(instance))
            elif inspect.isfunction(action):
                action(instance)
        await asyncio.gather(*coros)


def before_event(*event_types: Union[List[EventTypes], EventTypes]):
    """
    Decorator. It adds action, which should run before mentioned one
    or many events happen

    :param event_types: Union[List[EventTypes], EventTypes] - event types
    """
    return ActionRegistry.register(ActionDirections.BEFORE, *event_types)


def after_event(*event_types: Union[List[EventTypes], EventTypes]):
    """
    Decorator. It adds action, which should run after mentioned one
    or many events happen

    :param event_types: Union[List[EventTypes], EventTypes] - event types
    """
    return ActionRegistry.register(ActionDirections.AFTER, *event_types)


def wrap_with_actions(event_type: EventTypes):
    """
    Helper function to wrap Document methods with
    before and after event listeners
    :param event_type: EventTypes - event types
    :return: None
    """

    def decorator(f: Callable):
        @wraps(f)
        async def wrapper(self, *args, **kwargs):
            skip_actions = kwargs.setdefault("skip_actions", [])
            await ActionRegistry.run_actions(
                self,
                event_type,
                action_direction=ActionDirections.BEFORE,
                skip_actions=skip_actions,
            )
            result = await f(self, *args, **kwargs)
            await ActionRegistry.run_actions(
                self,
                event_type,
                action_direction=ActionDirections.AFTER,
                skip_actions=skip_actions,
            )
            return result

        return wrapper

    return decorator
