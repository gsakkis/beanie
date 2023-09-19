from dataclasses import dataclass, field
from typing import Any, Callable, Dict, NoReturn, Optional

from beanie.exceptions import StateManagementIsTurnedOff, StateNotSaved


@dataclass
class BaseDocumentState:
    __slots__ = ()

    def save(self) -> None:
        pass

    @property
    def saved(self) -> Optional[Dict[str, Any]]:
        return None

    @saved.setter
    def saved(self, value: Optional[Dict[str, Any]]) -> None:
        pass

    @property
    def previous_saved(self) -> Optional[Dict[str, Any]]:
        return None

    @property
    def is_changed(self) -> bool:
        self._raise()

    @property
    def has_changed(self) -> bool:
        self._raise(previous=True)

    def get_changes(self) -> Dict[str, Any]:
        self._raise()

    def get_previous_changes(self) -> Dict[str, Any]:
        self._raise(previous=True)

    def _raise(self, previous: bool = False) -> NoReturn:
        if previous:
            raise StateManagementIsTurnedOff(
                "state_management_save_previous is turned off for this document"
            )
        else:
            raise StateManagementIsTurnedOff(
                "State management is turned off for this document"
            )


@dataclass
class DocumentState(BaseDocumentState):
    _get_state: Callable[[], Dict[str, Any]] = field(repr=False, compare=False)
    _replace_objects: bool
    _saved: Optional[Dict[str, Any]] = None

    def save(self) -> None:
        self._saved = self._get_state()

    @property
    def saved(self) -> Optional[Dict[str, Any]]:
        return self._saved

    @saved.setter
    def saved(self, value: Optional[Dict[str, Any]]) -> None:
        self._saved = value

    @property
    def is_changed(self) -> bool:
        return self._ensure_saved() != self._get_state()

    @property
    def has_changed(self) -> bool:
        self._ensure_saved()
        return super().has_changed

    def get_changes(self) -> Dict[str, Any]:
        return self._collect_updates(self._ensure_saved(), self._get_state())

    def get_previous_changes(self) -> Dict[str, Any]:
        self._ensure_saved()
        return super().get_previous_changes()

    def _ensure_saved(self) -> Dict[str, Any]:
        if self._saved is None:
            raise StateNotSaved("No state was saved")
        return self._saved

    def _collect_updates(
        self, old_dict: Dict[str, Any], new_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compares old_dict with new_dict and returns field paths that have been updated
        Args:
            old_dict: dict1
            new_dict: dict2

        Returns: dictionary with updates
        """
        if old_dict.keys() - new_dict.keys():
            return new_dict
        updates = {}
        for name, new_value in new_dict.items():
            old_value = old_dict.get(name)
            if new_value == old_value:
                continue
            if (
                not self._replace_objects
                and isinstance(new_value, dict)
                and isinstance(old_value, dict)
            ):
                value_updates = self._collect_updates(old_value, new_value)
                for k, v in value_updates.items():
                    updates[f"{name}.{k}"] = v
            else:
                updates[name] = new_value
        return updates


@dataclass
class PreviousDocumentState(DocumentState):
    _previous_saved: Optional[Dict[str, Any]] = None

    def save(self) -> None:
        self._previous_saved = self._saved
        super().save()

    @property
    def previous_saved(self) -> Optional[Dict[str, Any]]:
        return self._previous_saved

    @property
    def has_changed(self) -> bool:
        return (
            self._previous_saved != self._ensure_saved()
            and self._previous_saved is not None
        )

    def get_previous_changes(self) -> Dict[str, Any]:
        if self._previous_saved is None:
            return {}
        return self._collect_updates(
            self._previous_saved, self._ensure_saved()
        )
