import inspect
from functools import wraps
from typing import Callable

from beanie.exceptions import StateManagementIsTurnedOff, StateNotSaved


def saved_state_needed(f: Callable):
    @wraps(f)
    def sync_wrapper(self, *args, **kwargs):
        _check_state(self, previous=False)
        return f(self, *args, **kwargs)

    @wraps(f)
    async def async_wrapper(self, *args, **kwargs):
        _check_state(self, previous=False)
        return await f(self, *args, **kwargs)

    return async_wrapper if inspect.iscoroutinefunction(f) else sync_wrapper


def previous_saved_state_needed(f: Callable):
    @wraps(f)
    def sync_wrapper(self, *args, **kwargs):
        _check_state(self, previous=True)
        return f(self, *args, **kwargs)

    @wraps(f)
    async def async_wrapper(self, *args, **kwargs):
        _check_state(self, previous=True)
        return await f(self, *args, **kwargs)

    return async_wrapper if inspect.iscoroutinefunction(f) else sync_wrapper


def save_state_after(f: Callable):
    @wraps(f)
    async def wrapper(self, *args, **kwargs):
        result = await f(self, *args, **kwargs)
        self.save_state()
        return result

    return wrapper


def swap_revision_after(f: Callable):
    @wraps(f)
    async def wrapper(self, *args, **kwargs):
        result = await f(self, *args, **kwargs)
        self.swap_revision()
        return result

    return wrapper


def _check_state(self, previous=False):
    settings = self.get_settings()
    if not settings.use_state_management:
        raise StateManagementIsTurnedOff(
            "State management is turned off for this document"
        )
    if previous:
        if not settings.state_management_save_previous:
            raise StateManagementIsTurnedOff(
                "State management's option to save previous state is turned off for this document"
            )
    else:
        if self._saved_state is None:
            raise StateNotSaved("No state was saved")
