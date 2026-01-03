from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from task_pipeline.pipeline.task.abstract_task import AbstractTask

@dataclass
class BaseTask(AbstractTask, ABC):
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
        return self.run(*args, **kwargs)

    def __rshift__(self, other: "BaseTask") -> "BaseTask":
        """
        Chain this task to the next task using ``>>``.

        Args:
            other (BaseTask): The next task in the pipeline.

        Returns:
            BaseTask: The chained task.
        """
        self.next_task = other
        return other

    def __lshift__(self, other: "BaseTask") -> "BaseTask":
        """
        Chain this task to the previous task using ``<<``.

        Args:
            other (BaseTask): The previous task in the pipeline.

        Returns:
            BaseTask: This task.
        """
        other.next_task = self
        return self
