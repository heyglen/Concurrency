# from nose.tools import , assert_true, assert_false, assert_raises, assert_is_instance

import time

from concurrent.futures import TimeoutError

from concurrency import Concurrency, Task

from nose.tools import assert_raises, assert_equals, assert_in


def deep_exception_fn(data):
    exception_fn(data)


def exception_fn(data):
    raise ValueError('Bad!')


def timeout_fn(data):
    time.sleep(0.1)
    return data


def normal_fn(data):
    return data


class TestConcurrency(object):
    def setup(self):
        pass

    def test_tread_normal(self):
        inputs = (0, 1, 2, 3)
        concurrent = Concurrency(normal_fn)
        for index, result in enumerate(concurrent.run(inputs)):
            assert_equals(result, index)

    def test_tread_exception(self):
        tasks = list()
        for task in (1, 2, 3):
            tasks.append(Task(task))

        concurrent = Concurrency(exception_fn)
        assert_raises(ValueError, concurrent.run, tasks)
        concurrent = Concurrency(deep_exception_fn)
        try:
            concurrent.run(tasks)
        except ValueError as e:
            e = str(e)
            assert_in('in deep_exception_fn', e)
            assert_in('in exception_fn', e)

    def test_tread_timeout(self):
        tasks = list()
        for task in (1, 2, 3):
            tasks.append(Task(task))
        concurrent = Concurrency(timeout_fn, timeout=0.05)
        # concurrent.run(tasks)
        assert_raises(TimeoutError, concurrent.run, tasks)
        # concurrent.run(tasks)


class TestProcssConcurrency(object):
    def setup(self):
        pass

    def test_process_normal(self):
        inputs = (0, 1, 2, 3)
        concurrent = Concurrency(normal_fn, concurrency_type='process')
        for index, result in enumerate(concurrent.run(inputs)):
            assert_equals(result, index)

    def tst_process_exception(self):
        tasks = list()
        for task in (1, 2, 3):
            tasks.append(Task(task))

        concurrent = Concurrency(exception_fn, concurrency_type='process')
        assert_raises(ValueError, concurrent.run, tasks)
        concurrent = Concurrency(deep_exception_fn, concurrency_type='process')
        try:
            concurrent.run(tasks)
        except ValueError as e:
            e = str(e)
            assert_in('in deep_exception_fn', e)
            assert_in('in exception_fn', e)

    def tst_process_timeout(self):
        tasks = list()
        for task in (1, 2, 3):
            tasks.append(Task(task))
        concurrent = Concurrency(timeout_fn, concurrency_type='process', timeout=0.05)
        # concurrent.run(tasks)
        assert_raises(TimeoutError, concurrent.run, tasks)
        # concurrent.run(tasks)
