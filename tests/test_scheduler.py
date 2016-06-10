import mock
import pytest

from greentasks import scheduler as mod


@pytest.fixture
def scheduler():
    return mod.TaskScheduler()


@mock.patch.object(mod, 'spawn_later')
def test__async(spawn_later, scheduler):
    fn = mock.Mock()
    scheduler._async(10, fn, 1, 2, a=3, b=4)
    spawn_later.assert_called_once_with(10, fn, 1, 2, a=3, b=4)


def test__execute(scheduler):
    packaged_task = mock.Mock()
    ret = scheduler._execute(packaged_task)
    assert packaged_task.run.called
    assert ret == packaged_task.run.return_value


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_execute')
def test__periodic_task_instantiation_fails(_execute, _async, scheduler):
    packaged_task = mock.Mock()
    _execute.return_value = None
    scheduler._periodic(packaged_task)
    assert not _async.called


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_execute')
def test__periodic_task_get_delay_fails(_execute, _async, scheduler):
    packaged_task = mock.Mock()
    task_instance = mock.Mock()
    _execute.return_value = task_instance
    task_instance.get_delay.side_effect = Exception()
    scheduler._periodic(packaged_task)
    assert task_instance.get_delay.called
    assert not _async.called


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_execute')
def test__periodic_no_delay(_execute, _async, scheduler):
    packaged_task = mock.Mock()
    task_instance = mock.Mock()
    _execute.return_value = task_instance
    task_instance.get_delay.return_value = None
    scheduler._periodic(packaged_task)
    assert task_instance.get_delay.called
    assert not _async.called


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_execute')
def test__periodic_has_delay(_execute, _async, scheduler):
    packaged_task = mock.Mock()
    task_instance = mock.Mock()
    _execute.return_value = task_instance
    task_instance.get_delay.return_value = 10
    scheduler._periodic(packaged_task)
    assert task_instance.get_delay.called
    _async.assert_called_once_with(10, scheduler._periodic, packaged_task)


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_execute')
def test__consume_empty_queue(_execute, _async, scheduler):
    scheduler._consume()
    assert not _execute.called
    _async.assert_called_once_with(scheduler._consume_tasks_delay,
                                   scheduler._consume)


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_execute')
def test__consume_task_found(_execute, _async, scheduler):
    first_task = mock.Mock()
    second_task = mock.Mock()
    scheduler._queue.put(first_task)
    scheduler._queue.put(second_task)
    scheduler._consume()
    _execute.assert_called_once_with(first_task)
    _async.assert_called_once_with(scheduler._consume_tasks_delay,
                                   scheduler._consume)
    assert scheduler._queue.peek() is second_task


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler.base_task_class, 'from_callable')
@mock.patch.object(mod.TaskScheduler, 'packaged_task_class')
def test_schedule_no_delay(packaged_task_class, from_callable, _async,
                           scheduler):
    (fn, callback, errback) = (mock.Mock(), mock.Mock(), mock.Mock())
    args = (1, 2)
    kwargs = dict(a=3)
    packaged_task_class.return_value.delay = None
    ret = scheduler.schedule(fn,
                             args=args,
                             kwargs=kwargs,
                             callback=callback,
                             errback=errback)
    task_cls = from_callable.return_value
    packaged_task_class.assert_called_once_with(task_cls,
                                                args=args,
                                                kwargs=kwargs,
                                                callback=callback,
                                                errback=errback)
    assert not _async.called
    assert scheduler._queue.peek() is packaged_task_class.return_value
    assert ret is packaged_task_class.return_value


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler.base_task_class, 'from_callable')
@mock.patch.object(mod.TaskScheduler, 'packaged_task_class')
def test_schedule_delayed_oneoff(packaged_task_class, from_callable, _async,
                                 scheduler):
    (fn, callback, errback) = (mock.Mock(), mock.Mock(), mock.Mock())
    args = (1, 2)
    kwargs = dict(a=3)
    delay = 10
    periodic = False
    packaged_task_class.return_value.delay = delay
    packaged_task_class.return_value.periodic = periodic
    ret = scheduler.schedule(fn,
                             args=args,
                             kwargs=kwargs,
                             callback=callback,
                             errback=errback)
    task_cls = from_callable.return_value
    packaged_task_class.assert_called_once_with(task_cls,
                                                args=args,
                                                kwargs=kwargs,
                                                callback=callback,
                                                errback=errback)
    _async.assert_called_once_with(delay,
                                   scheduler._execute,
                                   packaged_task_class.return_value)
    assert scheduler._queue.empty()
    assert ret is packaged_task_class.return_value


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler.base_task_class, 'from_callable')
@mock.patch.object(mod.TaskScheduler, 'packaged_task_class')
def test_schedule_delayed_periodic(packaged_task_class, from_callable, _async,
                                   scheduler):
    (fn, callback, errback) = (mock.Mock(), mock.Mock(), mock.Mock())
    args = (1, 2)
    kwargs = dict(a=3)
    delay = 10
    periodic = True
    packaged_task_class.return_value.delay = delay
    packaged_task_class.return_value.periodic = periodic
    ret = scheduler.schedule(fn,
                             args=args,
                             kwargs=kwargs,
                             callback=callback,
                             errback=errback)
    task_cls = from_callable.return_value
    packaged_task_class.assert_called_once_with(task_cls,
                                                args=args,
                                                kwargs=kwargs,
                                                callback=callback,
                                                errback=errback)
    _async.assert_called_once_with(delay,
                                   scheduler._periodic,
                                   packaged_task_class.return_value)
    assert scheduler._queue.empty()
    assert ret is packaged_task_class.return_value


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler.base_task_class, 'from_callable')
@mock.patch.object(mod.TaskScheduler.base_task_class, 'is_descendant')
def test_schedule_base_task_subclass(is_descendant, from_callable, _async,
                                     scheduler):
    task = mock.Mock()
    is_descendant.return_value = True
    scheduler.schedule(task, delay=10, periodic=True)
    assert not from_callable.called


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler.base_task_class, 'from_callable')
@mock.patch.object(mod.TaskScheduler.base_task_class, 'is_descendant')
def test_schedule_non_base_task_subclass(is_descendant, from_callable, _async,
                                         scheduler):
    task = mock.Mock()
    is_descendant.return_value = False
    scheduler.schedule(task, delay=10, periodic=True)
    from_callable.assert_called_once_with(task, delay=10, periodic=True)
