# from nose.tools import , assert_true, assert_false, assert_raises, assert_is_instance

import time
# import traceback

# from functools import partial
from concurrent.futures import TimeoutError

from concurrency import Concurrency, Task

from nose.tools import assert_raises, assert_equals, assert_in


def deep_value_exception_fn(data):
    value_exception_fn(data)


def value_exception_fn(data):
    raise ValueError('Bad!')


def timeout_fn(data):
    time.sleep(1)
    return data


def wrapper_fn(data, fn):
    return fn(data)


def normal_fn(data):
    return data


class TestThreadConcurrency(object):
    def setup(self):
        pass

    def test_normal(self):
        inputs = (0, 1, 2, 3)
        concurrent = Concurrency(normal_fn)
        for result in concurrent.run(inputs):
            assert_in(result, inputs)

    def test_fn_passing(self):
        inputs = (1, 2, 3)
        tasks = [Task(task, normal_fn) for task in inputs]
        concurrent = Concurrency(wrapper_fn)
        for result in concurrent.run(tasks):
            assert_in(result, inputs)

    def test_exception(self):
        tasks = list()
        for task in (1, 2, 3):
            tasks.append(Task(task))

        concurrent = Concurrency(value_exception_fn)
        results = concurrent.run(tasks)
        assert_raises(ValueError, next, results)

        concurrent = Concurrency(deep_value_exception_fn)
        try:
            concurrent.run(tasks)
        except ValueError as e:
            e = str(e)
            assert_in('in deep_value_exception_fn', e)
            assert_in('in value_exception_fn', e)

    def test_timeout(self):
        tasks = list()
        for task in (1, 2, 3):
            tasks.append(Task(task))
        concurrent = Concurrency(timeout_fn, timeout=0.05)
        futures = concurrent.run(tasks)
        assert_raises(TimeoutError, next, futures)


class TestProcessConcurrency(object):
    def setup(self):
        pass

    def test_normal(self):
        inputs = (0, 1, 2, 3)
        concurrent = Concurrency(normal_fn, concurrency_type='process')
        for result in concurrent.run(inputs):
            assert_in(result, inputs)

    def test_fn_passing(self):
        inputs = (1, 2, 3)
        tasks = [Task(task, normal_fn) for task in inputs]
        concurrent = Concurrency(wrapper_fn, concurrency_type='process')
        for result in concurrent.run(tasks):
            assert_in(result, inputs)

    def test_exception(self):
        tasks = [Task(task) for task in (1, 2, 3)]
        concurrent = Concurrency(value_exception_fn, concurrency_type='process')
        results = concurrent.run(tasks)
        assert_raises(ValueError, next, results)
        return
        concurrent = Concurrency(deep_value_exception_fn, concurrency_type='process')
        try:
            concurrent.run(tasks)
        except ValueError as e:
            e = str(e)
            assert_in('in deep_value_exception_fn', e)
            assert_in('in value_exception_fn', e)

    def test_timeout(self):
        tasks = list()
        for task in (1, 2, 3):
            tasks.append(Task(task))
        concurrent = Concurrency(timeout_fn, concurrency_type='process', timeout=0.05)
        futures = concurrent.run(tasks)
        assert_raises(TimeoutError, next, futures)
