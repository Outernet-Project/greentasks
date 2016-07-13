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


def test_task_get_start_delay():

    class TestTask(mod.Task):
        delay = 15

    task = TestTask()
    assert task.get_start_delay() == 15


def test_task_get_delay():

    class TestTask(mod.Task):
        delay = 15

    task = TestTask()
    assert task.get_delay(previous_delay=None) == 15


@pytest.mark.parametrize('retry_count, retry_delay', [
    (0, 15),
    (1, None),
    (2, None),
])
def test_task_get_retry_delay(retry_count, retry_delay):

    class TestTask(mod.Task):
        retry_delay = 15
        max_retries = 1

    task = TestTask()
    assert task.get_retry_delay(None, retry_count) == retry_delay


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
    assert task_cls.name == 'test_function'
    assert task_cls.__name__ == 'AutoGenTest_function'
    task = task_cls()
    task(1, 2, a=3, b=4)
    fn.assert_called_once_with(1, 2, a=3, b=4)


@pytest.mark.parametrize('task_cls,name', [
    (type('TestTask1', (mod.Task,), {}), 'TestTask1'),
    (type('TestTask2', (mod.Task,), {'name': 'Custom'}), 'Custom'),
])
def test_get_name(task_cls, name):
    assert task_cls.get_name() == name


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


@mock.patch.object(mod.PackagedTask.base_task_class, 'from_callable')
@mock.patch.object(mod.PackagedTask.base_task_class, 'is_descendant')
def test_init_with_base_task_subclass(is_descendant, from_callable):
    task = mock.Mock()
    is_descendant.return_value = True
    mod.PackagedTask(task)
    assert not from_callable.called


@mock.patch.object(mod.PackagedTask.base_task_class, 'from_callable')
@mock.patch.object(mod.PackagedTask.base_task_class, 'is_descendant')
def test_init_with_non_base_task_subclass(is_descendant, from_callable):
    task = mock.Mock()
    is_descendant.return_value = False
    mod.PackagedTask(task,
                     delay=10,
                     periodic=True,
                     retry_delay=5,
                     max_retries=3)
    from_callable.assert_called_once_with(task,
                                          delay=10,
                                          periodic=True,
                                          retry_delay=5,
                                          max_retries=3)


def test__generate_task_id():
    gen_id = mod.PackagedTask._generate_task_id()
    assert isinstance(gen_id, numbers.Number)


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test_packaged_task_name(base_task_class):
    mocked_task = mock.MagicMock()
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.name == mocked_task.get_name.return_value


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test_packaged_task_status(base_task_class):
    ptask = mod.PackagedTask(mock.Mock())
    assert ptask.status == mod.PackagedTask.SCHEDULED


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test__retry_fail(base_task_class):
    ptask = mod.PackagedTask(mock.Mock())
    task_instance = mock.Mock()
    task_instance.get_retry_delay.side_effect = Exception('test')
    assert ptask._retry(task_instance) == dict(delay=None)
    task_instance.get_retry_delay.assert_called_once_with(None, 0)


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test__retry_success(base_task_class):
    ptask = mod.PackagedTask(mock.Mock())
    task_instance = mock.Mock()
    retry_delay = task_instance.get_retry_delay.return_value
    assert ptask._retry(task_instance) == dict(delay=retry_delay)
    task_instance.get_retry_delay.assert_called_once_with(None, 0)
    assert ptask._previous_retry_delay == retry_delay
    assert ptask._retry_count == 1
    assert ptask._status == mod.PackagedTask.RETRY


@mock.patch.object(mod.PackagedTask, '_reschedule')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test__retry_exceeded(base_task_class, _reschedule):
    ptask = mod.PackagedTask(mock.Mock())
    ptask._previous_retry_delay = 10
    ptask._retry_count = 2
    ptask._status = mod.PackagedTask.FAILED
    task_instance = mock.Mock()
    task_instance.get_retry_delay.return_value = None
    assert ptask._retry(task_instance) is _reschedule.return_value
    task_instance.get_retry_delay.assert_called_once_with(10, 2)
    _reschedule.assert_called_once_with(task_instance)
    assert ptask._previous_retry_delay is None
    # make sure state is not changed
    assert ptask._retry_count == 2
    assert ptask._status == mod.PackagedTask.FAILED


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test__reschedule_fail(base_task_class):
    ptask = mod.PackagedTask(mock.Mock())
    task_instance = mock.Mock()
    task_instance.get_delay.side_effect = Exception('test')
    assert ptask._reschedule(task_instance) == dict(delay=None)
    task_instance.get_delay.assert_called_once_with(None)


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test__reschedule_success(base_task_class):
    ptask = mod.PackagedTask(mock.Mock())
    ptask._retry_count = 3
    task_instance = mock.Mock()
    delay = task_instance.get_delay.return_value
    assert ptask._reschedule(task_instance) == dict(delay=delay)
    task_instance.get_delay.assert_called_once_with(None)
    assert ptask._previous_delay == delay
    assert ptask._status == mod.PackagedTask.SCHEDULED
    assert ptask._retry_count == 0


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test__reschedule_not_needed(base_task_class):
    ptask = mod.PackagedTask(mock.Mock())
    task_instance = mock.Mock()
    task_instance.periodic = False
    assert ptask._reschedule(task_instance) == dict(delay=None)
    assert not task_instance.get_delay.called


@mock.patch.object(mod.PackagedTask, '_retry')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
@mock.patch.object(mod.PackagedTask, 'future_class')
def test__failed_instantiation(future_class, base_task_class, _retry):
    result = future_class.return_value
    mocked_task = mock.Mock()
    errback = mock.Mock()
    mocked_exc = mock.Mock()
    ptask = mod.PackagedTask(mocked_task, errback=errback)
    assert ptask._failed(None, mocked_exc) == {}
    result.set_exception.assert_called_once_with(mocked_exc)
    errback.assert_called_once_with(mocked_exc)
    assert ptask.status == mod.PackagedTask.FAILED
    assert not _retry.called


@mock.patch.object(mod.PackagedTask, '_retry')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
@mock.patch.object(mod.PackagedTask, 'future_class')
def test__failed_execution(future_class, base_task_class, _retry):
    result = future_class.return_value
    mocked_task = mock.Mock()
    task_instance = mocked_task.return_value
    errback = mock.Mock()
    mocked_exc = mock.Mock()
    ptask = mod.PackagedTask(mocked_task, errback=errback)
    assert ptask._failed(task_instance, mocked_exc) == _retry.return_value
    result.set_exception.assert_called_once_with(mocked_exc)
    errback.assert_called_once_with(mocked_exc)
    assert ptask.status == mod.PackagedTask.FAILED
    _retry.assert_called_once_with(task_instance)


@mock.patch.object(mod.PackagedTask, '_reschedule')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
@mock.patch.object(mod.PackagedTask, 'future_class')
def test__finished(future_class, base_task_class, _reschedule):
    result = future_class.return_value
    mocked_task = mock.Mock()
    task_instance = mocked_task.return_value
    callback = mock.Mock()
    ret_val = mock.Mock()
    ptask = mod.PackagedTask(mocked_task, callback=callback)
    ptask._finished(task_instance, ret_val)
    result.set.assert_called_once_with(ret_val)
    callback.assert_called_once_with(ret_val)
    assert ptask.status == mod.PackagedTask.FINISHED
    _reschedule.assert_called_once_with(task_instance)


@mock.patch.object(mod.PackagedTask, '_failed')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test_instantiate_fail(base_task_class, _failed):
    mocked_task = mock.MagicMock()
    exc = Exception('test')
    mocked_task.side_effect = exc
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.instantiate() is None
    _failed.assert_called_once_with(None, exc)


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test_instantiate_success(base_task_class):
    mocked_task = mock.MagicMock()
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.instantiate() == mocked_task.return_value


@mock.patch.object(mod.PackagedTask, 'instantiate')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test_run_failed_instantiation(base_task_class, instantiate):
    mocked_task = mock.MagicMock()
    instantiate.return_value = None
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.run() == {}


@mock.patch.object(mod.PackagedTask, '_failed')
@mock.patch.object(mod.PackagedTask, 'instantiate')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test_run_failed_execution(base_task_class, instantiate, _failed):
    mocked_task = mock.MagicMock()
    task_instance = instantiate.return_value
    exc = Exception("test")
    task_instance.side_effect = exc
    ptask = mod.PackagedTask(mocked_task)
    assert ptask.run() is _failed.return_value
    _failed.assert_called_once_with(task_instance, exc)


@mock.patch.object(mod.PackagedTask, '_finished')
@mock.patch.object(mod.PackagedTask, 'instantiate')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test_run_finished(base_task_class, instantiate, _finished):
    args = (1, 2)
    kwargs = dict(a=3, b=4)
    mocked_task = mock.MagicMock()
    task_instance = instantiate.return_value
    ptask = mod.PackagedTask(mocked_task, args, kwargs)
    assert ptask.run() is _finished.return_value
    task_instance.assert_called_once_with(*args, **kwargs)
    _finished.assert_called_once_with(task_instance,
                                      task_instance.return_value)


@mock.patch.object(mod.PackagedTask, '_generate_task_id')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test___hash__(base_task_class, _generate_task_id):
    _generate_task_id.return_value = 123456789
    ptask = mod.PackagedTask(mock.Mock())
    assert hash(ptask) == 123456789


@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test___eq__(base_task_class):
    ptask1 = mod.PackagedTask(mock.Mock())
    ptask2 = mod.PackagedTask(mock.Mock())
    assert ptask1 == ptask1
    assert ptask2 == ptask2
    assert ptask1 != ptask2


@mock.patch.object(mod.PackagedTask, '_generate_task_id')
@mock.patch.object(mod.PackagedTask, 'base_task_class')
def test___str__(base_task_class, _generate_task_id):
    mocked_task = mock.Mock()
    mocked_task.get_name.return_value = 'AwesomeTask'
    _generate_task_id.return_value = 123
    ptask = mod.PackagedTask(mocked_task)
    assert unicode(ptask) == '<PackagedTask: 123 - AwesomeTask>'
