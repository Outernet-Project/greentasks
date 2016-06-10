import inspect
import logging
import uuid

from gevent.event import AsyncResult


class Task(object):
    """
    Base task class, meant to be subclassed by implementors.
    """
    #: Fixed delay in which amount of time should the task run
    delay = None
    #: Indicates whether the task should be repeated or not
    periodic = False

    def get_delay(self):
        """
        Return the delay for periodic tasks in which amount of time they should
        run again. Subclasses may override this method and implement custom
        logic that calculates the value dynamically.

        Returning ``None`` from this method will stop the task from being
        rescheduled ever again.
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
    def from_callable(cls, fn, delay, periodic):
        """
        Generate a subclass of py:class:`Task` from the passed in callable by
        substituting the unimplemented py:meth:`~Task.run` method of the parent
        class with ``fn`` and attaching the other ``delay`` and ``periodic``
        attributes to it as well.
        """
        bases = (cls,)
        attrs = dict(delay=delay, periodic=periodic, run=staticmethod(fn))
        return type('AutoGen{}'.format(fn.__name__.capitalize()), bases, attrs)

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

    def __init__(self, task_cls, args, kwargs, callback, errback):
        self.task_cls = task_cls
        self.args = args or tuple()
        self.kwargs = kwargs or dict()
        self.callback = callback
        self.errback = errback
        self.result = AsyncResult()
        self.id = self._generate_task_id()
        self._status = self.SCHEDULED

    @staticmethod
    def _generate_task_id():
        """
        Return a unique string that can be used as an ID for tasks.
        """
        return uuid.uuid4().hex

    @property
    def name(self):
        """
        Return the name of the underlying py:attr:`~PackagedTask.task_cls`.
        """
        return self.task_cls.__name__

    @property
    def status(self):
        """
        Return the current status of the task.
        """
        return self._status

    @property
    def delay(self):
        """
        Return the value of py:attr:`~Task.delay`.
        """
        return self.task_cls.delay

    @property
    def periodic(self):
        """
        Return the value of py:attr:`~Task.periodic`.
        """
        return self.task_cls.periodic

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
        task_instance = None
        try:
            task_instance = self.task_cls()
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

    def __str__(self):
        ctx = dict(name=self.__class__.__name__,
                   task_name=self.name,
                   id=self.id)
        return '<{name}: {id} - {task_name}>'.format(**ctx)
