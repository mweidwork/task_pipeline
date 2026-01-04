from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, override

from task_pipeline.pipeline.event import (
    AbstractEvent,
    TaskEventType,
    post_task,
    pre_task,
)
from task_pipeline.pipeline.task.abstract_task import AbstractTask


@dataclass
class BaseTask(AbstractTask, AbstractEvent, ABC):
    """
    Base class for a pipeline task.

    Users should create their own task classes by subclassing
    ``BaseTask`` and implementing the `run` method.

    Tasks can be chained together to form a pipeline using
    the ``>>`` and ``<<`` operators.

    Examples:
        >>> class DoubleTask(BaseTask):
        ...     def run(self, x: int) -> int:
        ...         return x * 2
        ...
        >>> class IncrementTask(BaseTask):
        ...     def run(self, x: int) -> int:
        ...         return x + 1
        ...
        >>> double = DoubleTask(name="double")
        >>> inc = IncrementTask(name="increment")
        >>> double >> inc
        BaseTask(name='increment')
        >>> double(3)
        6
    """

    name: str
    next_task: BaseTask | None = field(default=None, repr=False)

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Any:
        """
        Run the task logic.

        This method must be implemented by subclasses.

        Args:
            *args: Input arguments for the task.
            **kwargs: Input keyword arguments for the task.

        Returns:
            Any: Result of the task.
        """
        raise NotImplementedError

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the task.

        Args:
            *args: Positional arguments passed to the task.
            **kwargs: Keyword arguments passed to the task.

        Returns:
            Any: Result of the task execution.
        """
        self._execute_event(TaskEventType.PRE, *args, **kwargs)
        result: Any = self.run(*args, **kwargs)
        self._execute_event(TaskEventType.POST, *args, **kwargs)
        return result

    @override
    def __rshift__(self, other: AbstractTask) -> BaseTask:
        """
        Chain this task to the next task using ``>>``.

        Args:
            other (AbstractTask): The next task in the pipeline.

        Returns:
            BaseTask: The chained task.

        Raises:
            TypeError: If `other` is not a BaseTask.
        """

        if not isinstance(other, BaseTask):
            raise TypeError("Can only chain BaseTask instances")

        self.next_task = other
        return other

    @override
    def __lshift__(self, other: AbstractTask) -> BaseTask:
        """
        Chain this task to the previous task using ``<<``.

        Args:
            other (AbstractTask): The previous task in the pipeline.

        Returns:
            BaseTask: This task.

        Raises:
            TypeError: If `other` is not a BaseTask.
        """

        if not isinstance(other, BaseTask):
            raise TypeError("Can only chain BaseTask instances")

        other.next_task = self
        return self

    @pre_task
    def _start(self, *args, **kwargs) -> None:
        print(f"[START] {self.name}")

    @post_task
    def _end(self, *args, **kwargs) -> None:
        print(f"[END] {self.name}")
