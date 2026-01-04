from enum import Enum, auto
from typing import Any


class TaskEventType(Enum):
    """
    Enumeration of task lifecycle event types.
    """

    PRE = auto()
    POST = auto()


class AbstractEvent:
    """
    Base class providing lifecycle event hook support.

    Subclasses can declare methods decorated with task event decorators
    (e.g. PRE, POST). Hooks are collected at class creation time and
    executed during the task lifecycle.
    """

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        # Initialize hook registry
        cls._hooks = {event: [] for event in TaskEventType}

        # Inherit hooks from base classes
        for base in cls.__mro__[1:]:
            if hasattr(base, "_hooks"):
                for event, hooks in base._hooks.items():
                    cls._hooks[event].extend(hooks)

        # Register hooks declared on this class
        for attr in cls.__dict__.values():
            if not callable(attr):
                continue

            event_type = getattr(attr, "__task_event_type__", None)
            if event_type is not None:
                cls._hooks[event_type].append(attr)

    def _execute_event(self, event: TaskEventType, *args: Any, **kwargs: Any) -> None:
        """
        Execute all hooks registered for the given event type.

        Args:
            event: Lifecycle event type to execute.
            *args: Positional arguments passed to the hook.
            **kwargs: Keyword arguments passed to the hook.
        """

        for hook in self._hooks[event]:
            hook(self, *args, **kwargs)
