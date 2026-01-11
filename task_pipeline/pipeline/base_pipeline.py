from typing import Any

from task_pipeline.pipeline.abstract_pipeline import AbstractPipeline


class BasePipeline(AbstractPipeline):
    """Base class for task pipelines"""

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Execute the pipeline starting from the root task.

        This method must be implemented by subclasses.
        """
        if not self.root_task:
            raise RuntimeError("No root task registered in the pipeline.")
        return self.root_task.run(*args, **kwargs)
