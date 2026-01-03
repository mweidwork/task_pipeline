from typing import Any, Callable
from .base_task import BaseTask


def task(*, task_type: type[BaseTask], name: str | None = None) -> Callable[[Callable[..., Any]], BaseTask]:
    """Decorator that converts a function into a ``BaseTask``.

    Args:
        task_type (type[BaseTask]): The subclass of BaseTask to instantiate.
        name (str | None): Optional explicit task name. Defaults to the function name.

    Returns:
        Callable[[Callable[..., Any]], BaseTask]: A task instance.

    Examples:
        >>> from .base_task import BaseTask
        >>> class AddTask(BaseTask):
        ...     def run(self, x: int, y: int) -> int:
        ...         return x + y
        ...
        >>> @task(task_type=AddTask)
        ... def add(x: int, y: int) -> int:
        ...     return x + y
        >>> add.name
        'add'
        >>> add(2, 3)
        5
    """

    def wrapper(fn: Callable[..., Any]) -> BaseTask:
        task_name = name or fn.__name__

        t = task_type(name=task_name)
        fn.__task__ = t

        return t

    return wrapper
