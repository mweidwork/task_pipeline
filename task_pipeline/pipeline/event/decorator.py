from collections.abc import Callable
from typing import Any, cast

from .abstract_event import TaskEventType


def task_event(event_type: TaskEventType):
    """
    Decorator to mark a method as a task lifecycle hook.

    The decorated method will be registered and executed when the
    specified task event occurs.

    Args:
        event_type: Task lifecycle event type.
    """

    def decorator(fn: Callable) -> Callable:
        """
        Attach task event metadata to the decorated function.

        Args:
            fn: Method to be registered as an event hook.

        Returns:
            The original function with event metadata attached.
        """
        setattr(fn, "__task_event_type__", event_type)
        return cast(Callable[..., Any], fn)

    return decorator


def pre_task(fn: Callable) -> Callable:
    """
    Decorator to register a pre-task lifecycle hook.

    The decorated method is executed before the task `run` method.
    """
    return task_event(TaskEventType.PRE)(fn)


def post_task(fn: Callable) -> Callable:
    """
    Decorator to register a post-task lifecycle hook.

    The decorated method is executed after the task `run` method.
    """
    return task_event(TaskEventType.POST)(fn)
