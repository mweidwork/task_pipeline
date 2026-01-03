from abc import ABC


class BasePipeline(ABC):
    """Abstract base class for task pipelines"""

    def __init__(self, name: str):
        self.name = name
        self.root_task: None = None

    def register_task(self, task):
        """Register the root task of the pipeline"""
        self.root_task = task
