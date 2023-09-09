import asyncio
import inspect
from enum import Enum
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Type,
    Union,
)

if TYPE_CHECKING:
    from beanie.odm.documents import Document


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
        Type["Document"],
        Dict[EventTypes, Dict[ActionDirections, List[Callable[..., Any]]]],
    ] = {}

    @classmethod
    def init_actions(cls, document_class: Type["Document"]) -> None:
        cls._actions[document_class] = {
            event_type: {direction: [] for direction in ActionDirections}
            for event_type in EventTypes
        }
        actions = cls._actions[document_class]
        for attr in dir(document_class):
            f = getattr(document_class, attr)
            if (
                inspect.isfunction(f)
                and hasattr(f, "event_types")
                and hasattr(f, "action_direction")
            ):
                for event_type in f.event_types:
                    actions[event_type][f.action_direction].append(f)

    @classmethod
    def get_action_list(
        cls,
        document_class: Type["Document"],
        event_type: EventTypes,
        action_direction: ActionDirections,
    ) -> List[Callable]:
        """
        Get stored action list
        :param document_class: Type - document class
        :param event_type: EventTypes - type of needed event
        :param action_direction: ActionDirections - before or after
        :return: List[Callable] - list of stored methods
        """
        if document_class not in cls._actions:
            return []
        return cls._actions[document_class][event_type][action_direction]

    @classmethod
    async def run_actions(
        cls,
        instance: "Document",
        event_type: EventTypes,
        action_direction: ActionDirections,
        exclude: List[Union[ActionDirections, str]],
    ):
        """
        Run actions
        :param instance: Document - object of the Document subclass
        :param event_type: EventTypes - event types
        :param action_direction: ActionDirections - before or after
        """
        if action_direction in exclude:
            return

        document_class = instance.__class__
        actions_list = cls.get_action_list(
            document_class, event_type, action_direction
        )
        coros = []
        for action in actions_list:
            if action.__name__ in exclude:
                continue

            if inspect.iscoroutinefunction(action):
                coros.append(action(instance))
            elif inspect.isfunction(action):
                action(instance)
        await asyncio.gather(*coros)


def register_action(
    event_types: Tuple[Union[List[EventTypes], EventTypes]],
    action_direction: ActionDirections,
):
    """
    Decorator. Base registration method.
    Used inside `before_event` and `after_event`
    :param event_types: Union[List[EventTypes], EventTypes] - event types
    :param action_direction: ActionDirections - before or after
    :return:
    """
    final_event_types = []
    for event_type in event_types:
        if isinstance(event_type, list):
            final_event_types.extend(event_type)
        else:
            final_event_types.append(event_type)

    def decorator(f):
        f.event_types = final_event_types
        f.action_direction = action_direction
        return f

    return decorator


def before_event(*args: Union[List[EventTypes], EventTypes]):
    """
    Decorator. It adds action, which should run before mentioned one
    or many events happen

    :param args: Union[List[EventTypes], EventTypes] - event types
    :return: None
    """
    return register_action(
        action_direction=ActionDirections.BEFORE, event_types=args  # type: ignore
    )


def after_event(*args: Union[List[EventTypes], EventTypes]):
    """
    Decorator. It adds action, which should run after mentioned one
    or many events happen

    :param args: Union[List[EventTypes], EventTypes] - event types
    :return: None
    """

    return register_action(
        action_direction=ActionDirections.AFTER, event_types=args  # type: ignore
    )


def wrap_with_actions(event_type: EventTypes):
    """
    Helper function to wrap Document methods with
    before and after event listeners
    :param event_type: EventTypes - event types
    :return: None
    """

    def decorator(f: Callable):
        @wraps(f)
        async def wrapper(
            self,
            *args,
            skip_actions: Optional[List[Union[ActionDirections, str]]] = None,
            **kwargs,
        ):
            if skip_actions is None:
                skip_actions = []

            await ActionRegistry.run_actions(
                self,
                event_type=event_type,
                action_direction=ActionDirections.BEFORE,
                exclude=skip_actions,
            )

            result = await f(self, *args, skip_actions=skip_actions, **kwargs)

            await ActionRegistry.run_actions(
                self,
                event_type=event_type,
                action_direction=ActionDirections.AFTER,
                exclude=skip_actions,
            )

            return result

        return wrapper

    return decorator
