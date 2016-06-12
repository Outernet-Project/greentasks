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
    task_instance.store_delay.assert_called_once_with(None)


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
    task_instance.store_delay.assert_called_once_with(10)


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
@mock.patch.object(mod.TaskScheduler, 'packaged_task_class')
def test_schedule_instantiation_fails(packaged_task_class, _async, scheduler):
    (fn, callback, errback) = (mock.Mock(), mock.Mock(), mock.Mock())
    args = (1, 2)
    kwargs = dict(a=3)
    packaged_task_class.return_value.instantiate.return_value = None
    ret = scheduler.schedule(fn,
                             args=args,
                             kwargs=kwargs,
                             callback=callback,
                             errback=errback)
    assert ret is None


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, 'packaged_task_class')
def test_schedule_no_delay(packaged_task_class, _async, scheduler):
    (fn, callback, errback) = (mock.Mock(), mock.Mock(), mock.Mock())
    args = (1, 2)
    kwargs = dict(a=3)
    task_instance = packaged_task_class.return_value.instantiate.return_value
    task_instance.get_start_delay.return_value = None
    ret = scheduler.schedule(fn,
                             args=args,
                             kwargs=kwargs,
                             callback=callback,
                             errback=errback)
    packaged_task_class.assert_called_once_with(fn,
                                                args=args,
                                                kwargs=kwargs,
                                                callback=callback,
                                                errback=errback,
                                                delay=None,
                                                periodic=False)
    assert not _async.called
    assert scheduler._queue.peek() is packaged_task_class.return_value
    assert ret is packaged_task_class.return_value


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, 'packaged_task_class')
def test_schedule_delayed_oneoff(packaged_task_class, _async, scheduler):
    (fn, callback, errback) = (mock.Mock(), mock.Mock(), mock.Mock())
    args = (1, 2)
    kwargs = dict(a=3)
    delay = 10
    periodic = False
    packaged_task_class.return_value.periodic = periodic
    task_instance = packaged_task_class.return_value.instantiate.return_value
    task_instance.get_start_delay.return_value = delay
    ret = scheduler.schedule(fn,
                             args=args,
                             kwargs=kwargs,
                             callback=callback,
                             errback=errback,
                             delay=delay,
                             periodic=periodic)
    packaged_task_class.assert_called_once_with(fn,
                                                args=args,
                                                kwargs=kwargs,
                                                callback=callback,
                                                errback=errback,
                                                delay=delay,
                                                periodic=periodic)
    _async.assert_called_once_with(delay,
                                   scheduler._execute,
                                   packaged_task_class.return_value)
    assert scheduler._queue.empty()
    assert ret is packaged_task_class.return_value


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, 'packaged_task_class')
def test_schedule_delayed_periodic(packaged_task_class, _async, scheduler):
    (fn, callback, errback) = (mock.Mock(), mock.Mock(), mock.Mock())
    args = (1, 2)
    kwargs = dict(a=3)
    delay = 10
    periodic = True
    packaged_task_class.return_value.periodic = periodic
    task_instance = packaged_task_class.return_value.instantiate.return_value
    task_instance.get_start_delay.return_value = delay
    ret = scheduler.schedule(fn,
                             args=args,
                             kwargs=kwargs,
                             callback=callback,
                             errback=errback,
                             delay=delay,
                             periodic=periodic)
    packaged_task_class.assert_called_once_with(fn,
                                                args=args,
                                                kwargs=kwargs,
                                                callback=callback,
                                                errback=errback,
                                                delay=delay,
                                                periodic=periodic)
    _async.assert_called_once_with(delay,
                                   scheduler._periodic,
                                   packaged_task_class.return_value)
    assert scheduler._queue.empty()
    assert ret is packaged_task_class.return_value
