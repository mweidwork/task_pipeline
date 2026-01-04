from task_pipeline.pipeline.event import (
    AbstractEvent,
    TaskEventType,
    post_task,
    pre_task,
)


def test_pre_task_hook_is_registered():
    class TestEvent(AbstractEvent):
        @pre_task
        def before(self):
            pass

    assert TaskEventType.PRE in TestEvent._hooks
    assert len(TestEvent._hooks[TaskEventType.PRE]) == 1
    assert TestEvent._hooks[TaskEventType.PRE][0].__name__ == "before"


def test_post_task_hook_is_registered():
    class TestEvent(AbstractEvent):
        @post_task
        def after(self, result):
            pass

    assert TaskEventType.POST in TestEvent._hooks
    assert len(TestEvent._hooks[TaskEventType.POST]) == 1
    assert TestEvent._hooks[TaskEventType.POST][0].__name__ == "after"


def test_hooks_are_executed():
    calls = []

    class TestEvent(AbstractEvent):
        @pre_task
        def before(self):
            calls.append("pre")

        @post_task
        def after(self, result):
            calls.append(f"post:{result}")

    event = TestEvent()
    event._execute_event(TaskEventType.PRE)
    event._execute_event(TaskEventType.POST, 42)

    assert calls == ["pre", "post:42"]


def test_hook_execution_order_is_preserved():
    calls = []

    class TestEvent(AbstractEvent):
        @pre_task
        def first(self):
            calls.append("first")

        @pre_task
        def second(self):
            calls.append("second")

    event = TestEvent()
    event._execute_event(TaskEventType.PRE)

    assert calls == ["first", "second"]


def test_hooks_are_isolated_between_subclasses():
    class EventA(AbstractEvent):
        @pre_task
        def hook_a(self):
            pass

    class EventB(AbstractEvent):
        @pre_task
        def hook_b(self):
            pass

    assert len(EventA._hooks[TaskEventType.PRE]) == 1
    assert len(EventB._hooks[TaskEventType.PRE]) == 1

    assert EventA._hooks[TaskEventType.PRE][0].__name__ == "hook_a"
    assert EventB._hooks[TaskEventType.PRE][0].__name__ == "hook_b"


def test_non_decorated_methods_are_not_registered():
    class TestEvent(AbstractEvent):
        def normal_method(self):
            pass

    for hooks in TestEvent._hooks.values():
        assert hooks == []
