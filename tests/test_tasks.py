import mock
import pytest

import greentasks.tasks as mod


# TASK TESTS


def test_task_get_delay():

    class TestTask(mod.Task):
        delay = 15

    task = TestTask()
    assert task.get_delay() == 15


@mock.patch.object(mod.Task, 'run')
def test_task___call__(run):
    task = mod.Task()
    task(1, 2, a=3, b=4)
    run.assert_called_once_with(1, 2, a=3, b=4)


def test_task_from_callable():
    fn = mock.Mock()
    fn.__name__ = 'test_function'
    task_cls = mod.Task.from_callable(fn, delay=10, periodic=True)
    assert task_cls.delay == 10
    assert task_cls.periodic is True
    assert task_cls.__name__ == 'AutoGenTest_function'
    task = task_cls()
    task(1, 2, a=3, b=4)
    fn.assert_called_once_with(1, 2, a=3, b=4)


@pytest.mark.parametrize('candidate,expected', [
    (1, False),
    (mock.Mock, False),
    (mock.Mock(), False),
    (lambda x: x, False),
    (mod.Task, True),
    (type('TestTask', (mod.Task,), {}), True),
])
def test_is_descendant(candidate, expected):
    assert mod.Task.is_descendant(candidate) is expected


# PACKAGED TASK TESTS


def test_packaged_task_name():
    # TODO: finish tests for PackagedTask
    raise NotImplementedError()
