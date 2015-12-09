# -*- coding: utf-8 -*-

# import time
import logging

import click
import concurrent.futures


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

    def __init__(self, fn, maximum_concurrency=5, concurrency_type='threading',
                 timeout=180, progress_bar=None, label=None, debug=False):
        self._timeout = timeout or Concurrency._timeout
        self._fn = fn
        self._maximum_concurrency = maximum_concurrency
        self._concurrency_type = concurrent.futures.ThreadPoolExecutor
        self._progress_bar = progress_bar
        if concurrency_type == 'process':
            self._concurrency_type = concurrent.futures.ProcessPoolExecutor
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
