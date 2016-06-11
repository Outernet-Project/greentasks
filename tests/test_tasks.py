import numbers

import mock
import pytest

import greentasks.tasks as mod

try:
    str = basestring
    unicode = unicode
except NameError:
    str = str
    unicode = str


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


def test__generate_task_id():
    gen_id = mod.PackagedTask._generate_task_id()
    assert isinstance(gen_id, numbers.Number)


def test_packaged_task_name():
    mocked_task = mock.MagicMock()
    mocked_task.__name__ = 'TestTask'
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.name == 'TestTask'


def test_packaged_task_stats():
    ptask = mod.PackagedTask(mock.Mock())
    assert ptask.status == mod.PackagedTask.SCHEDULED


def test_packaged_task_delay():
    mocked_task = mock.MagicMock()
    mocked_task.delay = 30
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.delay == 30


def test_packaged_task_periodic():
    mocked_task = mock.MagicMock()
    mocked_task.periodic = True
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.periodic is True


@mock.patch.object(mod.PackagedTask, 'future_class')
def test__failed(future_class):
    result = future_class.return_value
    mocked_task = mock.Mock()
    errback = mock.Mock()
    mocked_exc = mock.Mock()
    ptask = mod.PackagedTask(mocked_task, errback=errback)
    ptask._failed(mocked_exc)
    result.set_exception.assert_called_once_with(mocked_exc)
    errback.assert_called_once_with(mocked_exc)
    assert ptask.status == mod.PackagedTask.FAILED


@mock.patch.object(mod.PackagedTask, 'future_class')
def test__finished(future_class):
    result = future_class.return_value
    mocked_task = mock.Mock()
    callback = mock.Mock()
    ret_val = mock.Mock()
    ptask = mod.PackagedTask(mocked_task, callback=callback)
    ptask._finished(ret_val)
    result.set.assert_called_once_with(ret_val)
    callback.assert_called_once_with(ret_val)
    assert ptask.status == mod.PackagedTask.FINISHED


@mock.patch.object(mod.PackagedTask, '_failed')
def test_run_failed_instantiation(_failed):
    mocked_task = mock.MagicMock()
    mocked_task.__name__ = 'FailingTask'
    exc = Exception('test')
    mocked_task.side_effect = exc
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.run() is None
    _failed.assert_called_once_with(exc)


@mock.patch.object(mod.PackagedTask, '_failed')
def test_run_failed_execution(_failed):
    mocked_task = mock.MagicMock()
    mocked_task.__name__ = 'FailingTask'
    exc = Exception('test')
    mocked_task.return_value.side_effect = exc
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.run() is mocked_task.return_value
    _failed.assert_called_once_with(exc)


@mock.patch.object(mod.PackagedTask, '_finished')
def test_run_finished(_finished):
    args = (1, 2)
    kwargs = dict(a=3, b=4)
    mocked_task = mock.MagicMock()
    mocked_task.__name__ = 'FinishingTask'
    task_instance = mocked_task.return_value
    ptask = mod.PackagedTask(mocked_task, args, kwargs)
    assert ptask.run() is task_instance
    task_instance.assert_called_once_with(*args, **kwargs)
    _finished.assert_called_once_with(task_instance.return_value)


@mock.patch.object(mod.PackagedTask, '_generate_task_id')
def test___hash__(_generate_task_id):
    _generate_task_id.return_value = 123456789
    ptask = mod.PackagedTask(mock.Mock())
    assert hash(ptask) == 123456789


def test___eq__():
    ptask1 = mod.PackagedTask(mock.Mock())
    ptask2 = mod.PackagedTask(mock.Mock())
    assert ptask1 == ptask1
    assert ptask2 == ptask2
    assert ptask1 != ptask2


@mock.patch.object(mod.PackagedTask, '_generate_task_id')
def test___str__(_generate_task_id):
    mocked_task = mock.Mock()
    mocked_task.__name__ = 'AwesomeTask'
    _generate_task_id.return_value = 123
    ptask = mod.PackagedTask(mocked_task)
    assert unicode(ptask) == '<PackagedTask: 123 - AwesomeTask>'
