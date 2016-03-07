try:
    import __builtin__ as builtins
except ImportError:
    import builtins

import mock
import pytest

original_import = builtins.__import__


def py3_gevent_import(name, *args, **kwargs):
    if name == 'gevent':
        return mock.Mock()
    return original_import(name, *args, **kwargs)

with mock.patch.object(builtins, '__import__', side_effect=py3_gevent_import):
    from greentasks import scheduler as mod


@pytest.fixture
def scheduler():
    return mod.TaskScheduler()


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_generate_task_id')
def test_schedule_no_delay(_generate_task_id, _async, scheduler):
    fn = mock.Mock()
    args = (1, 2)
    kwargs = dict(a=3)
    scheduler.schedule(fn, args=args, kwargs=kwargs)
    expected = (fn, args, kwargs)
    assert scheduler._queue[_generate_task_id.return_value] == expected
    assert not _async.called


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_generate_task_id')
def test_schedule_delayed_oneoff(_generate_task_id, _async, scheduler):
    fn = mock.Mock()
    args = (1, 2)
    kwargs = dict(a=3)
    delay = 10
    scheduler.schedule(fn, args=args, kwargs=kwargs, delay=delay)
    _async.assert_called_once_with(delay,
                                   scheduler._execute,
                                   _generate_task_id.return_value,
                                   fn,
                                   args,
                                   kwargs)


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_generate_task_id')
def test_schedule_delayed_periodic(_generate_task_id, _async, scheduler):
    fn = mock.Mock()
    args = (1, 2)
    kwargs = dict(a=3)
    delay = 10
    scheduler.schedule(fn,
                       args=args,
                       kwargs=kwargs,
                       delay=delay,
                       periodic=True)
    _async.assert_called_once_with(delay,
                                   scheduler._periodic,
                                   _generate_task_id.return_value,
                                   fn,
                                   args,
                                   kwargs,
                                   delay)


def test__execute(scheduler):
    fn = mock.Mock()
    args = (1, 2)
    kwargs = dict(a=3)
    scheduler._execute('taskid', fn, args, kwargs)
    fn.assert_called_once_with(*args, **kwargs)


@mock.patch.object(mod.TaskScheduler, '_async')
@mock.patch.object(mod.TaskScheduler, '_execute')
def test__periodic(_execute, _async, scheduler):
    task_id = 'taskid'
    fn = mock.Mock()
    args = (1, 2)
    kwargs = dict(a=3)
    delay = 10
    scheduler._periodic(task_id, fn, args, kwargs, delay)
    _execute.assert_called_once_with(task_id, fn, args, kwargs)
    # make sure periodic task reschedules itself after it's execution
    _async.assert_called_once_with(delay,
                                   scheduler._periodic,
                                   task_id,
                                   fn,
                                   args,
                                   kwargs,
                                   delay)


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
    fn = mock.Mock()
    args = (1, 2)
    kwargs = dict(a=3)
    scheduler._queue['first'] = (fn, args, kwargs)
    scheduler._queue['second'] = (1, 2, 3)
    scheduler._consume()
    _execute.assert_called_once_with('first', fn, args, kwargs)
    _async.assert_called_once_with(scheduler._consume_tasks_delay,
                                   scheduler._consume)
    assert 'second' in scheduler._queue


def test_get_status(scheduler):
    scheduler._queue['waitingid'] = 1
    scheduler.current_task = 'workingid'
    scheduler.get_status('waitingid') == scheduler.QUEUED
    scheduler.get_status('workingid') == scheduler.PROCESSING
    scheduler.get_status('unknown') == scheduler.NOT_FOUND
