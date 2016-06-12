import inspect
import logging
import uuid

from gevent.event import AsyncResult


class Task(object):
    """
    Base task class, meant to be subclassed by implementors.
    """
    #: Name of the task
    name = None
    #: Fixed delay in which amount of time should the task run. Every time the
    #: task class is instantiated, the value of py:attr:`~Task.delay` is going
    #: to be set to the value that was obtained from py:meth:`~Task.get_delay`
    #: on the previous run.
    delay = None
    #: Indicates whether the task should be repeated or not
    periodic = False

    def get_start_delay(self):
        """
        Return the amount of time in which this task should run for the first
        time. Subclasses may override this method and implement custom logic
        that calculates the value dynamically.
        """
        return self.delay

    def get_delay(self):
        """
        Return the delay for periodic tasks in which amount of time they should
        run again. Subclasses may override this method and implement custom
        logic that calculates the value dynamically.

        Returning ``None`` from this method will stop the task from being
        rescheduled again.
        """
        return self.delay

    def run(self, *args, **kwargs):
        """
        Subclasses should override this method and implement the task logic.
        """
        raise NotImplementedError()

    def __call__(self, *args, **kwargs):
        """
        Forwards all calls to py:meth:`~Task.run`, making the task instance
        callable.
        """
        return self.run(*args, **kwargs)

    @classmethod
    def store_delay(cls, delay):
        """
        Store ``delay`` on the py:attr:`~Task.delay` class-attribute, so on
        it's subsequent instantiation the value will be accessible.
        """
        cls.delay = delay

    @classmethod
    def from_callable(cls, fn, delay, periodic):
        """
        Generate a subclass of py:class:`Task` from the passed in callable by
        substituting the unimplemented py:meth:`~Task.run` method of the parent
        class with ``fn`` and attaching the other ``delay`` and ``periodic``
        attributes to it as well.
        """
        bases = (cls,)
        name = fn.__name__
        attrs = dict(name=name,
                     delay=delay,
                     periodic=periodic,
                     run=staticmethod(fn))
        return type('AutoGen{}'.format(name.capitalize()), bases, attrs)

    @classmethod
    def get_name(cls):
        """
        Return explicitly specified name of task, falling back to name of the
        class itself, if not specified.
        """
        return cls.name or cls.__name__

    @classmethod
    def is_descendant(cls, candidate):
        """
        Return whether the passed in ``candidate`` is a subclass of
        py:class:`Task` or not.
        """
        return inspect.isclass(candidate) and issubclass(candidate, cls)


class PackagedTask(object):
    """
    Wrapper object for tasks that capture the callable object, it's arguments
    and contain the optional callback and errback functions.

    The task's status can be queried through the py:attr:`~PackagedTask.status`
    property.

    The py:attr:`~PackagedTask.result` is an instance of py:class:`AsyncResult`
    and can be used as a regular future/promise to obtain the return value (or
    the exception that was raised) of an asynchronous task.
    """
    SCHEDULED = 'SCHEDULED'
    PROCESSING = 'PROCESSING'
    FAILED = 'FAILED'
    FINISHED = 'FINISHED'

    #: Base class used to implement the actual task logic
    base_task_class = Task

    #: Type of future object to use
    future_class = AsyncResult

    def __init__(self, task, args=None, kwargs=None, callback=None,
                 errback=None, delay=None, periodic=False):
        if not self.base_task_class.is_descendant(task):
            # convert the passed in callable into a subclass of py:class:`Task`
            task = self.base_task_class.from_callable(task,
                                                      delay=delay,
                                                      periodic=periodic)
        self.task_cls = task
        self.args = args or tuple()
        self.kwargs = kwargs or dict()
        self.callback = callback
        self.errback = errback
        self.result = self.future_class()
        self.id = self._generate_task_id()
        self._status = self.SCHEDULED

    @staticmethod
    def _generate_task_id():
        """
        Return a unique number that can be used as an ID for tasks.
        """
        return int(uuid.uuid4().hex, 16)

    @property
    def name(self):
        """
        Return the name of the underlying py:attr:`~PackagedTask.task_cls`.
        """
        return self.task_cls.get_name()

    @property
    def status(self):
        """
        Return the current status of the task.
        """
        return self._status

    def _failed(self, exc):
        """
        Set py:attr:`~PackagedTask._status` flag to indicate failure, resolve
        the future with the passed in `exc` exception object and invoke the
        optional py:attr:`~PackagedTask.errback` with the same `exc` object.
        """
        self._status = self.FAILED
        self.result.set_exception(exc)
        if self.errback:
            self.errback(exc)

    def _finished(self, ret_val):
        """
        Set py:attr:`~PackagedTask._status` flag to indicate success, resolve
        the future with ``ret_val`` - the return value of the task and invoke
        the optional py:attr:`~PackagedTask.callback` with ``ret_val``.
        """
        self._status = self.FINISHED
        self.result.set(ret_val)
        if self.callback:
            self.callback(ret_val)

    def instantiate(self):
        """
        Yield a new instance of the stored py:class:`Task` class, or ``None``
        in case instantiation fails.
        """
        try:
            return self.task_cls()
        except Exception as exc:
            logging.exception("Task[%s][%s] instantiation failed.",
                              self.name,
                              self.id)
            self._failed(exc)
            return None

    def run(self):
        """
        Execute the stored task, silencing and logging any exceptions it might
        raise. The py:attr:`~PackagedTask.result` future will be resolved with
        the return value (or exception object if it fails) of the task.

        A callback or errback function can additionally be invoked (if they are
        specified).
        """
        # mark the start of the task
        self._status = self.PROCESSING
        # in case task instantation fails, make sure the finally clause won't
        # be trying to access an undefined variable
        task_instance = self.instantiate()
        if not task_instance:
            return task_instance
        # task instantiation succeeded, start execution
        try:
            ret_val = task_instance(*self.args, **self.kwargs)
        except Exception as exc:
            logging.exception("Task[%s][%s] execution failed.",
                              self.name,
                              self.id)
            self._failed(exc)
        else:
            logging.info("Task[%s][%s] execution finished.",
                         self.name,
                         self.id)
            self._finished(ret_val)
        finally:
            # return the task instance used to execute the task
            return task_instance

    def __hash__(self):
        return self.id

    def __eq__(self, other):
        return hash(self.id) == hash(other.id)

    def __str__(self):
        ctx = dict(name=self.__class__.__name__,
                   task_name=self.name,
                   id=self.id)
        return '<{name}: {id} - {task_name}>'.format(**ctx)
