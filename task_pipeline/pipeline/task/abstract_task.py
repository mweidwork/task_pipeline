from abc import ABC
from dataclasses import dataclass
from typing import Any, Self


@dataclass
class AbstractTask(ABC):
    """
    Abstract class for a pipeline task.
    """

    name: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the task.

        This calls the :meth:`run` method by default.

        Args:
            *args: Positional arguments for the task.
            **kwargs: Keyword arguments for the task.

        Returns:
            Any: Result of task execution.
        """
        raise NotImplementedError

    def __rshift__(self, other: "AbstractTask") -> Self:
        """Chain this task to the next task using the ``>>`` operator.

        Args:
            other (AbstractTask): Task to execute after this task.

        Returns:
            AbstractTask: The next task in the pipeline.
        """
        raise NotImplementedError

    def __lshift__(self, other: "AbstractTask") -> Self:
        """Chain this task to the previous task using the ``<<`` operator.

        Args:
            other (AbstractTask): Task to execute before this task.

        Returns:
            AbstractTask: This task.
        """
        raise NotImplementedError
