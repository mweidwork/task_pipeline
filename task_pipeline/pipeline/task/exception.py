class ConditionTaskResolutionError(Exception):
    """Raised when a ConditionTask fails to resolve the next task.

    This occurs if a branch callable does not return a valid AbstractTask
    or if the target is otherwise invalid.
    """

    def __init__(self, branch_name: str, value: Any, func: Any):
        """
        Args:
            branch_name (str): The name of the branch (on_true, on_false, etc.).
            value (Any): The value that failed to resolve.
            func (Any): The callable or object used to resolve the next task.
        """
        func_info = getattr(func, "__name__", repr(func))
        message = (
            f"Failed to resolve next task for branch '{branch_name}'. "
            f"Expected an AbstractTask instance, got {type(value).__name__!r} instead. "
            f"Callable logic: {func_info}"
        )
        super().__init__(message)
        self.branch_name = branch_name
        self.value = value
        self.func = func
