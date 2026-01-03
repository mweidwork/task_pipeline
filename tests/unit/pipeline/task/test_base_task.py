import pytest
from dataclasses import dataclass
from task_pipeline.pipeline.task import BaseTask

# Concrete test subclasses
@dataclass
class DummyTask(BaseTask):
    def run(self, x):
        return x * 2

@dataclass
class AddTask(BaseTask):
    def run(self, x):
        return x + 5

# --- Fixtures ---
@pytest.fixture
def dummy():
    """Provides a fresh DummyTask."""
    return DummyTask(name="dummy")

@pytest.fixture
def adder():
    """Provides a fresh AddTask."""
    return AddTask(name="adder")

# --- Parametrized __call__ tests ---
@pytest.mark.parametrize(
    "task_class,input_val,expected",
    [
        (DummyTask, 2, 4),
        (AddTask, 3, 8),
    ],
)
def test_call_outputs(task_class, input_val, expected):
    """Test __call__ invokes run correctly for various tasks."""
    task = task_class(name="t")
    assert task(input_val) == expected

# --- Test chaining operations ---
def test_right_shift_chains(dummy, adder):
    next_task = dummy >> adder
    assert dummy.next_task is adder  # chaining established
    assert next_task is adder        # >> returns the next task

def test_left_shift_references(dummy, adder):
    left_returned = adder << dummy
    assert dummy.next_task is adder   # << sets previous as next_task
    assert left_returned is adder     # << returns self

# --- Chaining multiple tasks in sequence ---
def test_multiple_chain_sequence(dummy, adder):
    third = DummyTask(name="third")
    dummy >> adder >> third
    assert dummy.next_task is adder
    assert adder.next_task is third
    assert third.next_task is None

# --- Test chaining using >> ---
@pytest.mark.parametrize(
    "first_class, second_class",
    [
        (DummyTask, AddTask),
        (AddTask, DummyTask),
    ],
)
def test_right_shift_chaining(first_class, second_class):
    """Test that >> sets the next_task correctly and returns the next task."""
    a = first_class(name="a")
    b = second_class(name="b")
    returned = a >> b

    # >> should set a.next_task to b and return b
    assert a.next_task is b
    assert returned is b


# --- Test chaining using << ---
@pytest.mark.parametrize(
    "first_class, second_class",
    [
        (DummyTask, AddTask),
        (AddTask, DummyTask),
    ],
)
def test_left_shift_chaining(first_class, second_class):
    """Test that << sets the next_task correctly and returns self."""
    a = first_class(name="a")
    b = second_class(name="b")
    returned = a << b

    # << should set b.next_task to a and return a
    assert b.next_task is a
    assert returned is a

# --- Ensuring BaseTask is abstract ---
def test_base_task_abstract():
    with pytest.raises(TypeError):
        BaseTask(name="abstract-only")
