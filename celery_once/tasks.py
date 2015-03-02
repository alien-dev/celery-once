# -*- coding: utf-8 -*-
from celery import Task
# from inspect import getcallargs
import sys
from .helpers import queue_once_key, get_redis, now_unix


class AlreadyQueued(Exception):
    def __init__(self, countdown):
        self.message = "Expires in %d seconds" % countdown
        self.countdown = countdown


class QueueOnce(Task):
    AlreadyQueued = AlreadyQueued
    once = {
        'graceful': False,
    }

    """
    'There can be only one'. - Highlander (1986)
    An abstract tasks with the ability to detect if it has already been queued.
    When running the task (through .delay/.apply_async) it checks if the tasks
    is not already queued. By default it will raise an
    an AlreadyQueued exception if it is, by you can silence this by including
    `options={'graceful': True}` in apply_async or in the task's settings.
    Example:
    >>> from celery_queue.tasks import QueueOnce
    >>> from celery import task
    >>> @task(base=QueueOnce, once={'graceful': True})
    >>> def example(time):
    >>>     from time import sleep
    >>>     sleep(time)
    """
    abstract = True
    once = {}

    @property
    def config(self):
        app = self._get_app()
        return app.conf

    @property
    def redis(self):
        return get_redis(
            getattr(self.config, "ONCE_REDIS_URL", "redis://localhost:6379/0"))

    @property
    def default_timeout(self):
        return getattr(
            self.config, "ONCE_DEFAULT_TIMEOUT", 60 * 60)

    def apply_async(self, args=None, kwargs=None, **options):
        """
        Queues a task, raises an exception by default if already queued.
        :param \*args: positional arguments passed on to the task.
        :param \*\*kwargs: keyword arguments passed on to the task.
        :keyword \*\*once: (optional)
            :param: graceful: (optional)
                If True, wouldn't raise an exception if already queued.
                Instead will return none.
            :param: timeout: (optional)
                An `int' number of seconds after which the lock will expire.
                If not set, defaults to 1 hour.
            :param: keys: (optional)
        """
        once_options = options.get('once', {})
        once_graceful = once_options.get(
            'graceful', self.once.get('graceful', False))
        once_timeout = once_options.get(
            'timeout', self.once.get('timeout', self.default_timeout))

        key = self.get_key(args, kwargs)
        try:
            self.raise_or_lock(key, once_timeout)
        except self.AlreadyQueued as e:
            if once_graceful:
                return None
            raise e
        return super(QueueOnce, self).apply_async(args, kwargs, **options)

    def get_key(self, args=None, kwargs=None):
        """
        Generate the key from the name of the task (e.g. 'tasks.example') and
        args/kwargs.
        """
        restrict_to = self.once.get('keys', None)
        args = args or {}
        kwargs = kwargs or {}
        call_args = getcallargs(self.run, *args, **kwargs)
        # Remove the task instance from the kwargs. This only happens when the
        # task has the 'bind' attribute set to True. We remove it, as the task
        # has a memory pointer in its repr, that will change between the task
        # caller and the celery worker
        if isinstance(call_args.get('self'), Task):
            del call_args['self']
        key = queue_once_key(self.name, call_args, restrict_to)
        return key

    def raise_or_lock(self, key, expires):
        """
        Checks if the task is locked and raises an exception, else locks
        the task.
        """
        now = now_unix()
        # Check if the tasks is already queued if key is in redis.
        result = self.redis.get(key)
        if result:
            # Work out how many seconds remaining till the task expires.
            remaining = int(result) - now
            if remaining > 0:
                raise self.AlreadyQueued(remaining)

        # By default, the tasks and redis key expire after 60 minutes.
        # (meaning it will not be executed and the lock will clear).
        self.redis.setex(key, expires, now + expires)

    def clear_lock(self, key):
        self.redis.delete(key)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        """
        After a task has run (both succesfully or with a failure) clear the
        lock.
        """
        key = self.get_key(args, kwargs)
        self.clear_lock(key)

from inspect import getargspec, ismethod


# This is a copy of the inspect.getcallargs() function from Python 2.7
# so we can provide it for use under Python 2.6. As the code in this
# file derives from the Python distribution, it falls under the version
# of the PSF license used for Python 2.7.
def getcallargs(func, *positional, **named):
    """
    Get the mapping of arguments to values.
    A dict is returned, with keys the function argument names (including the
    names of the * and ** arguments, if any), and values the respective bound
    values from 'positional' and 'named'.
    """
    args, varargs, varkw, defaults = getargspec(func)
    f_name = func.__name__
    arg2value = {}

    # The following closures are basically because of
    # tuple parameter unpacking.
    assigned_tuple_params = []

    def assign(arg, value):
        if isinstance(arg, str):
            arg2value[arg] = value
        else:
            assigned_tuple_params.append(arg)
            value = iter(value)
            for i, subarg in enumerate(arg):
                try:
                    subvalue = next(value)
                except StopIteration:
                    raise ValueError('need more than %d %s to unpack' %
                                     (i, 'values' if i > 1 else 'value'))
                assign(subarg, subvalue)
            try:
                next(value)
            except StopIteration:
                pass
            else:
                raise ValueError('too many values to unpack')

    def is_assigned(arg):
        if isinstance(arg, str):
            return arg in arg2value
        return arg in assigned_tuple_params

    if ismethod(func) and func.im_self is not None:
        # implicit 'self' (or 'cls' for classmethods) argument
        positional = (func.im_self,) + positional

    num_pos = len(positional)
    num_total = num_pos + len(named)
    num_args = len(args)
    num_defaults = len(defaults) if defaults else 0
    for arg, value in zip(args, positional):
        assign(arg, value)
    if varargs:
        if num_pos > num_args:
            assign(varargs, positional[-(num_pos-num_args):])
        else:
            assign(varargs, ())
    elif 0 < num_args < num_pos:
        raise TypeError('%s() takes %s %d %s (%d given)' % (
            f_name, 'at most' if defaults else 'exactly', num_args,
            'arguments' if num_args > 1 else 'argument', num_total))
    elif num_args == 0 and num_total:
        if varkw:
            if num_pos:
                # XXX: We should use num_pos, but Python also uses num_total:
                raise TypeError('%s() takes exactly 0 arguments '
                                '(%d given)' % (f_name, num_total))
        else:
            raise TypeError('%s() takes no arguments (%d given)' %
                            (f_name, num_total))
    for arg in args:
        if isinstance(arg, str) and arg in named:
            if is_assigned(arg):
                raise TypeError("%s() got multiple values for keyword "
                                "argument '%s'" % (f_name, arg))
            else:
                assign(arg, named.pop(arg))
    if defaults:    # fill in any missing values with the defaults
        for arg, value in zip(args[-num_defaults:], defaults):
            if not is_assigned(arg):
                assign(arg, value)
    if varkw:
        assign(varkw, named)
    elif named:
        unexpected = next(iter(named))
        if isinstance(unexpected, unicode):
            unexpected = unexpected.encode(sys.getdefaultencoding(), 'replace')
        raise TypeError("%s() got an unexpected keyword argument '%s'" %
                        (f_name, unexpected))
    unassigned = num_args - len([arg for arg in args if is_assigned(arg)])
    if unassigned:
        num_required = num_args - num_defaults
        raise TypeError('%s() takes %s %d %s (%d given)' % (
            f_name, 'at least' if defaults else 'exactly', num_required,
            'arguments' if num_required > 1 else 'argument', num_total))
    return arg2value
