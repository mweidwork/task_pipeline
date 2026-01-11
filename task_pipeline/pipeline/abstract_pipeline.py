from abc import ABC, abstractmethod
from typing import Any

from task_pipeline.pipeline.task.base_task import BaseTask


class AbstractPipeline(ABC):
    """Abstract base class for task pipelines"""

    def __init__(self, name: str):
        self.name = name
        self.root_task: BaseTask | None = None

    def register_task(self, task: BaseTask) -> None:
        """
        Register the root task of the pipeline.

        Args:
            task (BaseTask): The first task in the pipeline.
        """
        if not isinstance(task, BaseTask):
            raise TypeError("Only BaseTask instances can be registered.")
        self.root_task = task

    @abstractmethod
    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the pipeline starting from the root task.

        This method must be implemented by subclasses.
        """
        raise NotImplementedError
