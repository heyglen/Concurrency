# from nose.tools import assert_equals, assert_true, assert_false, assert_raises, assert_is_instance

from concurrency import Concurrency


def exception_fn():
    raise ValueError('Bad!')


def normal_fn(input):
    return input


class TestConcurrency(object):
    def setup(self):
        pass

    def test_normal(self):
        inputs = (1, 2, 3)
        runner = Concurrency(normal_fn)
        runner.run(inputs)

    def tet_exception(self):
        inputs = (1, 2, 3)
        runner = Concurrency(exception_fn)
        runner.run(inputs)
