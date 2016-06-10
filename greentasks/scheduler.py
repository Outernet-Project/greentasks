import inspect
import logging

from gevent import spawn_later
from gevent.queue import Queue, Empty as QueueEmpty

from .tasks import PackagedTask, Task


class TaskScheduler(object):
    """
    A very simple task scheduler built on top of gevent.

    Allows scheduling of periodic, one-off delayed and one-off ordered tasks.

    Periodic tasks will run repeatedly after their execution in the specified
    amount of time.

    One-off delayed tasks are executed only once after the specified delay has
    passed.

    One-off ordered tasks are put into a queue and are executed in the same
    order they were scheduled. The queue is processed in fixed intervals which
    is specified by the ``consume_tasks_delay`` parameter in the constructor of
    the scheduler. In each wakeup, exactly one task will be processed.
    """
    #: Wrapper class enclosing all the data needed for a single task
    packaged_task_class = PackagedTask

    #: Base class used to implement the actual task logic
    base_task_class = Task

    def __init__(self, consume_tasks_delay=1):
        self._queue = Queue()
        self._consume_tasks_delay = consume_tasks_delay
        self._async(self._consume_tasks_delay, self._consume)

    def _async(self, delay, fn, *args, **kwargs):
        """
        Schedule a function with the passed in parameters to be executed
        asynchronously by gevent.
        """
        return spawn_later(delay, fn, *args, **kwargs)

    def _execute(self, packaged_task):
        """
        Delegate execution of ``packaged_task`` to py:meth:`~PackageTask.run`,
        which handles the invocation of appropriate callbacks and errbacks and
        the resolving of the future object.

        The return value is forwarded as is, which is the task instance itself,
        unless instantiation failed for some reason.
        """
        return packaged_task.run()

    def _periodic(self, packaged_task):
        """
        Execute a periodic task through py:meth:`~TaskScheduler._execute` and
        reschedule it automatically for the specified ``delay``.
        """
        task_instance = self._execute(packaged_task)
        if task_instance is None:
            # task instantiation probably failed, so it cannot be repeated
            return
        # attempt retrieving the ``delay`` which indicates in which amount of
        # time should the task run again
        try:
            delay = task_instance.get_delay()
        except Exception:
            logging.exception("Task[%s][%s] `get_delay` failed, no further "
                              "rescheduling will take place.",
                              packaged_task.id,
                              packaged_task.name)
        else:
            if delay is None:
                # task does not wish to be rescheduled again
                return
            # task needs to be rescheduled again
            self._async(delay, self._periodic, packaged_task)

    def _consume(self):
        """
        Execute a single task from the queue, and reschedule consuming of the
        queue in py:attr:`~_consume_tasks_delay`.
        """
        try:
            packaged_task = self._queue.get_nowait()
        except QueueEmpty:
            pass  # no task in the queue
        else:
            self._execute(packaged_task)
        finally:
            self._async(self._consume_tasks_delay, self._consume)

    def schedule(self, task, args=None, kwargs=None, callback=None,
                 errback=None, delay=None, periodic=False):
        """
        Schedule a task for execution and return the task object for it.

        If ``delay`` is not specified, the task will be put into a queue and
        honor the existing order of scheduled tasks, being executed only after
        the tasks scheduled prior to it are completed.

        If ``delay`` is specified, the task will be scheduled to run NOT BEFORE
        the specified amount of seconds, not following any particular order,
        but there is no guarantee that it will run in exactly that time.

        The ``periodic`` flag has effect only on tasks which specified a
        ``delay``, and those tasks will be rescheduled automatically for the
        same ``delay`` every time after they are executed.

        :param task:      Callable to execute
        :param args:      Tuple containing positional arguments for ``task``
        :param kwargs:    Dict containing keyword arguments for ``task``
        :param callback:  Function to invoke with return value of ``task``
        :param errback:   Function to invoke with exception if ``task`` fails
        :param delay:     Int - amount of seconds to execute ``task`` in
        :param periodic:  Boolean flag indicating if ``task`` is repeatable
        """
        if not self.base_task_class.is_descendant(task):
            # convert the passed in callable into a subclass of py:class:`Task`
            task = self.base_task_class.from_callable(task,
                                                      delay=delay,
                                                      periodic=periodic)
        # package task with all of it's arguments
        packaged_task = self.packaged_task_class(task,
                                                 args=args,
                                                 kwargs=kwargs,
                                                 callback=callback,
                                                 errback=errback)
        if packaged_task.delay is None:
            # schedule an ordered, one-off task
            self._queue.put(packaged_task)
            # early return with packaged task object to simplify flow
            return packaged_task
        # async task, order does not matter
        if packaged_task.periodic:
            # schedule a period task
            self._async(packaged_task.delay, self._periodic, packaged_task)
        else:
            # schedule a one-off task
            self._async(packaged_task.delay, self._execute, packaged_task)
        # return packaged task object in both cases
        return packaged_task
