from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Callable
from abc import ABC

from . import AbstractTask, BaseTask, ConditionTaskResolutionError




@dataclass
class ConditionTask(AbstractTask, ABC):
    """
    A pipeline task that executes conditional branching.

    This task evaluates a boolean function and selects the next task
    based on its result. Branches include `on_true`, `on_false`,
    `on_else` (if the function returns None), and `on_exception`.

    Attributes:
        fn (Callable[..., bool]): Function that determines the branch.
        on_true (BaseTask): Task executed if `fn` returns True.
        on_false (BaseTask): Task executed if `fn` returns False.
        on_exception (BaseTask | None): Task executed if `fn` raises an exception.
        on_else (BaseTask | None): Task executed if `fn` returns None.
    """

    fn: Callable[..., bool]
    on_true: BaseTask
    on_false: BaseTask
    on_exception: BaseTask | None = field(default=None, repr=False)
    on_else: BaseTask | None = field(default=None, repr=False)

    def resolver(self, *args: Any, **kwargs: Any) -> BaseTask:
        """
        Resolve the next task based on the function result.

        Evaluates `fn` and returns the appropriate next task. Raises
        `ConditionTaskResolutionError` if the result cannot be resolved.

        Args:
            *args: Positional arguments passed to `fn`.
            **kwargs: Keyword arguments passed to `fn`.

        Returns:
            BaseTask: The resolved next task.

        Raises:
            ConditionTaskResolutionError: If resolution fails.
        """
        try:
            result: bool | None = self.fn(*args, **kwargs)

            if result is None and self.on_else:
                return self.on_else
            elif result:
                return self.on_true
            else:
                return self.on_false

        except Exception as exc:
            if self.on_exception:
                return self.on_exception
            raise ConditionTaskResolutionError(branch_name=self.name, value=result, func=self.fn) from exc

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the conditional task and forward arguments to the next task.

        Returns:
            Any: The result of executing the resolved next task.
        """
        next_task = self.resolver(*args, **kwargs)
        return next_task(*args, **kwargs)
