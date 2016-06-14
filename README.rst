==========
greentasks
==========

A very simplistic task scheduler built around gevent.

-----
Usage
-----

1. Create a task scheduler instance::

    from greentasks import TaskScheduler

    scheduler = TaskScheduler()

2. Implement a task::

    from greentasks import Task

    class AwesomeTask(Task):
        name = 'awesome'
        delay = 10
        periodic = True

        def run(self, arg1, kw=None):
            return arg1

3. Schedule the task for asynchronous execution::

    packaged_task = scheduler.schedule(AwesomeTask)

4. Wait for the result (if needed)::

    result = packaged_task.result.get()
