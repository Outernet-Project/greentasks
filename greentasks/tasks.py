import inspect
import logging
import uuid

from gevent.event import AsyncResult


class Task(object):
    """
    Base task class, meant to be subclassed by implementors.
    """
    #: Prefix used for task name when converting a task from a callable
    auto_generated_name_prefix = 'AutoGen'
    #: Name of the task
    name = None
    #: Fixed delay in which amount of time should the task run.
    delay = None
    #: Indicates whether the task should be repeated or not
    periodic = False
    #: Maximum number of retries a task can rescheduled if it fails
    max_retries = 0
    #: Fixed delay in which amount of time can a task run again if it fails
    retry_delay = None

    def get_start_delay(self):
        """
        Return the amount of time in which this task should run for the first
        time. Subclasses may override this method and implement custom logic
        that calculates the value dynamically.
        """
        return self.delay

    def get_delay(self, previous_delay):
        """
        Return the delay for periodic tasks in which amount of time they should
        run again. Subclasses may override this method and implement custom
        logic that calculates the value dynamically.

        The parameter ``previous_delay`` indicates the value returned by
        py:meth:`~Task.get_delay` in it's previous run. During the first run of
        a periodic task, this value will be ``None``.

        Returning ``None`` from this method will stop the task from being
        rescheduled again.
        """
        return self.delay

    def get_retry_delay(self, previous_retry_delay, retry_count):
        """
        Return the amount of time in which a task can run again if it fails.
        Subclasses may override this method and implement custom logic that
        calculates the value dynamically.

        The parameter ``previous_retry_delay`` indicates the value returned by
        py:meth:`~Task.get_retry_delay` during the last call to it.

        The parameter ``retry_count`` indicates the number of times the task
        was already retried.

        Returning ``None`` from this method means the task cannot be retried.
        """
        if retry_count < self.max_retries:
            return self.retry_delay
        # max retries exceeded
        return None

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
    def from_callable(cls, fn, **kwargs):
        """
        Generate a subclass of py:class:`Task` from the passed in callable by
        substituting the unimplemented py:meth:`~Task.run` method of the parent
        class with ``fn`` and attaching ``name`` and all the passed ``kwargs``
        as class attributes to it as well.
        """
        bases = (cls,)
        name = fn.__name__
        attrs = dict(run=staticmethod(fn), name=name, **kwargs)
        return type(cls.auto_generated_name_prefix + name.capitalize(),
                    bases,
                    attrs)

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
    RETRY = 'RETRY'

    #: Base class used to implement the actual task logic
    base_task_class = Task

    #: Type of future object to use
    future_class = AsyncResult

    def __init__(self, task, args=None, kwargs=None, callback=None,
                 errback=None, delay=None, periodic=False, retry_delay=None,
                 max_retries=0):
        if not self.base_task_class.is_descendant(task):
            # convert the passed in callable into a subclass of py:class:`Task`
            task = self.base_task_class.from_callable(task,
                                                      delay=delay,
                                                      periodic=periodic,
                                                      retry_delay=retry_delay,
                                                      max_retries=max_retries)
        self.task_cls = task
        self.args = args or tuple()
        self.kwargs = kwargs or dict()
        self.callback = callback
        self.errback = errback
        self.result = self.future_class()
        self.id = self._generate_task_id()
        # private attributes that track the state of a task
        self._previous_delay = None
        self._previous_retry_delay = None
        self._retry_count = 0
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

    def _retry(self, task_instance):
        """
        Attempt querying the delay in which amount of time should the task be
        retried, and if it succeeds, set the state of py:class:`PackagedTask`
        to indicate that the task is being retried. If the query fails, return
        appropriate information to indicate that the task cannot be retried.
        """
        try:
            delay = task_instance.get_retry_delay(self._previous_retry_delay,
                                                  self._retry_count)
        except Exception:
            logging.exception("Task[%s][%s] `get_retry_delay` failed, task "
                              "cannot be retried.",
                              self.name,
                              self.id)
            return dict(delay=None)
        else:
            # store the current calculated retry delay so it can be accessed by
            # the next task instance
            self._previous_retry_delay = delay
            if delay is None:
                # if no further retries are to be made, check if rescheduling
                # is still needed though
                return self._reschedule(task_instance)
            # correct state to account for the currect retry
            self._retry_count += 1
            self._status = self.RETRY
            logging.debug("Task[%s][%s] retry #%s will run in %s seconds.",
                          self.name,
                          self.id,
                          self._retry_count,
                          delay)
            return dict(delay=delay)

    def _reschedule(self, task_instance):
        """
        Attempt querying the delay in which amount of time should the task be
        rescheduled. If it succeeds, set the state of py:class:`PackagedTask`
        to indicate that the task is rescheduled. If the query fails, return
        appropriate information to indicate that the task can't be rescheduled.
        """
        if not task_instance.periodic:
            return dict(delay=None)
        # it is a periodic task, query delay
        try:
            delay = task_instance.get_delay(self._previous_delay)
        except Exception:
            logging.exception("Task[%s][%s] `get_delay` failed, no further "
                              "rescheduling will take place.",
                              self.name,
                              self.id)
            return dict(delay=None)
        else:
            logging.debug("Task[%s][%s] rescheduled to run in %s seconds.",
                          self.name,
                          self.id,
                          delay)
            # store the current calculated delay so it can be accessed by the
            # next task instance
            self._previous_delay = delay
            self._status = self.SCHEDULED
            self._retry_count = 0
            return dict(delay=delay)

    def _failed(self, task_instance, exc):
        """
        Set py:attr:`~PackagedTask._status` flag to indicate failure, resolve
        the future with the passed in `exc` exception object and invoke the
        optional py:attr:`~PackagedTask.errback` with the same `exc` object.
        """
        self._status = self.FAILED
        self.result.set_exception(exc)
        if self.errback:
            self.errback(exc)
        # check whether task instantiation failed or not
        if task_instance is None:
            # instantiation failed, no retry attempt should be made
            return dict()
        # attempt retrying the task, if possible
        return self._retry(task_instance)

    def _finished(self, task_instance, ret_val):
        """
        Set py:attr:`~PackagedTask._status` flag to indicate success, resolve
        the future with ``ret_val`` - the return value of the task and invoke
        the optional py:attr:`~PackagedTask.callback` with ``ret_val``.
        """
        self._status = self.FINISHED
        self.result.set(ret_val)
        if self.callback:
            self.callback(ret_val)
        # attempt rescheduling the task, if needed and possible
        return self._reschedule(task_instance)

    def instantiate(self):
        """
        Return a new instance of the stored py:class:`Task` class, or ``None``
        in case instantiation fails.
        """
        try:
            return self.task_cls()
        except Exception as exc:
            logging.exception("Task[%s][%s] instantiation failed.",
                              self.name,
                              self.id)
            self._failed(None, exc)
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
        task_instance = self.instantiate()
        if not task_instance:
            return {}
        # task instantiation succeeded, start execution
        try:
            ret_val = task_instance(*self.args, **self.kwargs)
        except Exception as exc:
            logging.exception("Task[%s][%s] execution failed.",
                              self.name,
                              self.id)
            return self._failed(task_instance, exc)
        else:
            logging.debug("Task[%s][%s] execution finished.",
                          self.name,
                          self.id)
            return self._finished(task_instance, ret_val)

    def __hash__(self):
        return self.id

    def __eq__(self, other):
        return hash(self.id) == hash(other.id)

    def __str__(self):
        ctx = dict(name=self.__class__.__name__,
                   task_name=self.name,
                   id=self.id)
        return '<{name}: {id} - {task_name}>'.format(**ctx)
