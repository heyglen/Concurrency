# -*- coding: utf-8 -*-

# import time
import sys
import logging
import traceback

import click
import concurrent.futures


class ProcessPoolExecutorStackTraced(concurrent.futures.ProcessPoolExecutor):

    def submit(self, fn, *args, **kwargs):
        """Submits the wrapped function instead of `fn`"""

        return super(ProcessPoolExecutorStackTraced, self).submit(
            self._function_wrapper, fn, *args, **kwargs)

    def _function_wrapper(self, fn, *args, **kwargs):
        """Wraps `fn` in order to preserve the traceback of any kind of
        raised exception

        """
        try:
            return fn(*args, **kwargs)
        except Exception:
            raise sys.exc_info()[0](traceback.format_exc())


class ThreadPoolExecutorStackTraced(concurrent.futures.ThreadPoolExecutor):

    def submit(self, fn, *args, **kwargs):
        """Submits the wrapped function instead of `fn`"""

        return super(ThreadPoolExecutorStackTraced, self).submit(
            self._function_wrapper, fn, *args, **kwargs)

    def _function_wrapper(self, fn, *args, **kwargs):
        """Wraps `fn` in order to preserve the traceback of any kind of
        raised exception

        """
        try:
            return fn(*args, **kwargs)
        except Exception:
            raise sys.exc_info()[0](traceback.format_exc())


class Task(object):
    def __init__(self, item, *args, **kwargs):
        self.item = item
        self.args = args
        self.kwargs = kwargs


class no_progress_bar(object):
    @staticmethod
    def update(value):
        pass

    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass


class Concurrency(object):
    _timeout = 180

    def __init__(self, fn, maximum_concurrency=5, concurrency_type='thread',
                 timeout=180, progress_bar=None, label=None, debug=False):
        self._timeout = timeout or Concurrency._timeout
        self._fn = fn
        self._maximum_concurrency = maximum_concurrency
        # self._concurrency_type = concurrent.futures.ThreadPoolExecutor
        self._concurrency_type = ThreadPoolExecutorStackTraced
        self._progress_bar = progress_bar
        if concurrency_type == 'process':
            # self._concurrency_type = concurrent.futures.ProcessPoolExecutor
            self._concurrency_type = ProcessPoolExecutorStackTraced
        self._task_label = label
        self._debug = debug
        self._exception_callback = None
        self._timeout_callback = None
        self._log_setup(name='concurrency')

    def _log_setup(self, name=None):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s'))
        self._logger.addHandler(handler)

    def _single_run(self, tasks):
        results = list()
        for task in tasks:
            results.append(self._fn(task))
        return results

    def _setup_progress_bar(self, length, label):
        if not self._progress_bar:
            return no_progress_bar()
        return click.progressbar(length=length, label=label)

    def set_exception_callback(self, fn):
        self._exception_callback = fn

    def set_timeout_callback(self, fn):
        self._timeout_callback = fn

    def run(self, tasks):
        if self._debug:
            return self._single_run(tasks)
        results = list()
        label = None
        if self._progress_bar:
            label = self._task_label

        with self._concurrency_type(max_workers=self._maximum_concurrency) as executor:
            progress_bar = self._setup_progress_bar(len(tasks), label)
            with progress_bar as progress:
                for result in executor.map(self._fn, tasks, timeout=self._timeout):
                    results.append(result)
                    progress.update(1)
        return results
