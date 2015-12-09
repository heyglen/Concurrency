# -*- coding: utf-8 -*-
import time
import logging

import click
import concurrent.futures

class RunInstance(object):
    def __init__(self, fn, inputs, *args, **kwargs):
        self.fn = fn
        self.input = inputs
        self.args = args
        self.kwargs = kwargs


class Concurrency(object):
    _timeout = 180
    def __init__(self, fn, maximum_concurrency=5, concurrency_type='threading', timeout=180, progress_bar=None, label=None, debug=False):
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

    def _single_run(self, inputs, *args, **kwargs):
        results = list()
        for _input in inputs:
            results.append(self._fn(_input, *args, **kwargs))
        return results

    def set_exception_callback(self, fn):
        self._exception_callback = fn

    def set_timeout_callback(self, fn):
        self._timeout_callback = fn

    def run(self, inputs, *args, **kwargs):
        if self._debug: return self._single_run(inputs, *args, **kwargs)
        futures = list()
        retries = list()
        results = list()
        label = None
        if self._progress_bar: label = self._task_label
        
        with self._concurrency_type(max_workers=self._maximum_concurrency) as executor:
            for _input in inputs:
                run = RunInstance(self._fn, _input, args, kwargs)
                futures.append((executor.submit(run.fn, run.input, *run.args, **run.kwargs), run))
            progress_bar = click.progressbar(length=len(futures), label=label)
            with progress_bar as progress:
                while len(futures):
                    for future, run in futures:
                        exception = None
                        result = None
                        try:
                            raised_exception = future.exception(timeout=self._timeout)
                            _input = run.input
                            if self._exception_callback:
                                self._exception_callback(raised_exception)
                            self._logger.error('{0}: {1}'.format(type(raised_exception).__name__, raised_exception))
                            retries.append(run)
                        except concurrent.futures.TimeoutError:
                            pass
                        try:
                            result = future.result(timeout=self._timeout)
                            results.append(result)
                            if self._progress_bar: progress.update(1)
                        except concurrent.futures.TimeoutError:
                            self._logger.debug('Timeout')
                            if self._timeout_callback is not None:
                                self._timeout_callback(run.input)
                    futures = []
                    self._logger.debug('Retrying {0} failed runs'.format(len(retries)))
                    for run in retries:
                        #fn, _input, args, kwargs = run
                        futures.append((executor.submit(run.fn, run.input, *run.args, **run.kwargs), run))
        return results


