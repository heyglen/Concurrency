# -*- coding: utf-8 -*-

# import time
import sys
import logging
import traceback
from contextlib import contextmanager
import inspect

import dill
import click
import concurrent.futures
from functools import partial


class ProcessPoolExecutorStackTraced(concurrent.futures.ProcessPoolExecutor):

    @staticmethod
    def _dump_pickle_args(*args, **kwargs):
        pickled_args = list()

        for index, arg in enumerate(args):
            if inspect.isclass(arg):
                pickled_args.append(index)

        for arg in pickled_args:
            args[arg] = dill.dumps(args[arg])

        kwargs['_pickled_args'] = pickled_args

    @staticmethod
    def _load_pickle_args(*args, **kwargs):
        pickled_args = kwargs.get('_pickled_args')
        if pickled_args:
            for arg in pickled_args:
                args[arg] = dill.loads(args[arg])

    def submit(self, fn, *args, **kwargs):
        """Submits the wrapped function instead of `fn`"""
        # args = list()
        # kwargs = dict()

        ProcessPoolExecutorStackTraced._dump_pickle_args(args, kwargs)
        return super(ProcessPoolExecutorStackTraced, self).submit(
            ProcessPoolExecutorStackTraced._fn_wrapper, fn, *args, **kwargs)

    @staticmethod
    def _fn_wrapper(fn, *args, **kwargs):
        """Wraps `fn` in order to preserve the traceback of any kind of
        raised exception
        """
        fn = dill.loads(fn)
        ProcessPoolExecutorStackTraced._load_pickle_args(args, kwargs)
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            return type(e)(traceback.format_exc())
            # raise sys.exc_info()[0](traceback.format_exc())


class ThreadPoolExecutorStackTraced(concurrent.futures.ThreadPoolExecutor):

    def submit(self, fn, *args, **kwargs):
        """Submits the wrapped function instead of `fn`"""
        return super(ThreadPoolExecutorStackTraced, self).submit(
            self._fn_wrapper, fn, *args, **kwargs)

    def _fn_wrapper(self, fn, *args, **kwargs):
        """Wraps `fn` in order to preserve the traceback of any kind of
        raised exception
        """
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            return type(e)(traceback.format_exc())
            # raise sys.exc_info()[0](traceback.format_exc())


class Task(object):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


@contextmanager
def no_progress_bar():
    class NoProgress(object):
        @staticmethod
        def update(value):
            pass
    yield NoProgress


class Concurrency(object):
    _timeout = 180

    def __init__(self, fn, maximum_concurrency=5, concurrency_type='thread',
                 timeout=None, progress_bar=None, label=None, debug=False):
        self._timeout = timeout if timeout is not None else Concurrency._timeout
        self._maximum_concurrency = maximum_concurrency
        self._debug = debug
        self._set_concurrency_type(concurrency_type, fn)
        self._progress_bar = progress_bar
        self._task_label = label
        self._exception_callback = None
        self._timeout_callback = None
        self._log_setup(name='concurrency')

    def _set_concurrency_type(self, concurrency_type, fn):
        # self._concurrency_type = ThreadPoolExecutorStackTraced
        self._concurrency_type = concurrent.futures.ThreadPoolExecutor
        self._pickle = False
        self._fn = fn
        if concurrency_type == 'process' and not self._debug:
            # self._concurrency_type = ProcessPoolExecutorStackTraced
            self._concurrency_type = concurrent.futures.ProcessPoolExecutor
            self._pickle = True
            self._fn = dill.dumps(fn)

    def _log_setup(self, name=None):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
        self._logger.addHandler(handler)

    def _setup_progress_bar(self, length):
        label = None
        if self._progress_bar:
            label = self._task_label
        if not self._progress_bar:
            return no_progress_bar()
        return click.progressbar(length=length, label=label)

    @staticmethod
    def _values_to_tasks(values):
        """ Convert values to Task objects """
        tasks = list()
        for task in values:
            if not isinstance(task, Task):
                tasks.append(Task(task))
            else:
                tasks.append(task)
        return tasks

    @staticmethod
    def _submit_tasks(executor, fn, pickled, tasks):
        """ Submits the function providing the correct amount of arguments """
        # fn = partial(_fn_wrapper, fn)
        futures = set()
        for task in tasks:
            future = None
            # Run the function depending on it's required arguments
            if task.args and task.kwargs:
                future = executor.submit(_fn_wrapper, fn, pickled, *task.args, **task.kwargs)
            elif task.args:
                future = executor.submit(_fn_wrapper, fn, pickled, *task.args)
            else:
                future = executor.submit(_fn_wrapper, fn, pickled)
            futures.add(future)
        return futures

    @staticmethod
    def _process_future(future):
        result = future.result()
        if isinstance(result, Exception):
            raise result
        return result

    def run(self, tasks):
        """ Run synconously if in debug mode """
        runner = self._single_run if self._debug else self._run
        tasks = Concurrency._values_to_tasks(tasks)
        progress_bar = self._setup_progress_bar(len(tasks))
        for result in runner(tasks, progress_bar):
            yield result

    def _run(self, tasks, progress_bar):
        """ Asyncronously runs the provided tasks """
        with self._concurrency_type(max_workers=self._maximum_concurrency) as executor:
            with progress_bar as progress:
                futures = Concurrency._submit_tasks(executor, self._fn, self._pickle, tasks)
                for future in concurrent.futures.as_completed(futures, timeout=self._timeout):
                    progress.update(1)
                    yield Concurrency._process_future(future)

    def _single_run(self, tasks, progress_bar):
        with progress_bar as progress:
            for task in tasks:
                progress.update(1)
                yield self._fn(*task.args, **task.kwargs)


def _fn_wrapper(fn, pickled, *args, **kwargs):
    """Wraps `fn` in order to preserve the traceback of any kind of
    raised exception
    """
    if pickled:
        fn = dill.loads(fn)
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        return type(e)(traceback.format_exc())
        # raise sys.exc_info()[0](traceback.format_exc())
