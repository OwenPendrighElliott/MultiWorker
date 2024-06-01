from workercontext import parallelise
import pytest
from typing import List


@pytest.fixture
def short_list():
    return [1, 2, 3, 4, 5, 6, 7, 8]


@pytest.fixture
def long_list():
    return list(range(1_000_000))


def func_no_kwargs(l: List[int], a: int, b: int):
    for i in range(len(l)):
        l[i] = l[i] + a + b
    return l


def func_second_arg_list(a: int, l: List[int], b: int):
    for i in range(len(l)):
        l[i] = l[i] + a + b
    return l


def func_kwargs(l: List[int], a: int = 3, b: int = 5):
    for i in range(len(l)):
        l[i] = l[i] + a + b
    return l


def func_only_kwargs(l: List[int], a: int = 3, b: int = 5):
    for i in range(len(l)):
        l[i] = l[i] + a + b
    return l


def func_no_list(a, b, c):
    return a + b + c


def test_one_worker(short_list):
    res = parallelise(func_no_kwargs, n_processes=1)(short_list, 1, 1)
    assert res[0][0] == 3 and res[0][-1] == 10
    assert len(res[0]) == len(short_list)


def test_workers(short_list, long_list):
    res = parallelise(func_no_kwargs, n_processes=2)(short_list, 1, 1)
    assert res[0][0] == 3 and res[-1][-1] == 10
    assert sum(map(len, res)) == len(short_list)

    res = parallelise(func_no_kwargs, n_processes=2)(long_list, 1, 1)
    assert res[0][0] == 2 and res[-1][-1] == 1_000_001
    assert sum(map(len, res)) == len(long_list)


def test_with_kwargs(short_list):
    res = parallelise(func_no_kwargs, n_processes=2)(short_list, b=1, a=1)
    assert res[0][0] == 3 and res[-1][-1] == 10
    assert sum(map(len, res)) == len(short_list)


def test_only_kwargs(short_list):
    res = parallelise(func_no_kwargs, n_processes=2)(l=short_list, b=1, a=1)
    assert res[0][0] == 3 and res[-1][-1] == 10
    assert sum(map(len, res)) == len(short_list)


def test_second_arg_list(short_list):
    res = parallelise(func_no_kwargs, n_processes=2)(l=short_list, b=1, a=1)
    assert res[0][0] == 3 and res[-1][-1] == 10
    assert sum(map(len, res)) == len(short_list)


def test_invalid_param_error():
    failed = False
    try:
        res = parallelise(func_no_kwargs, n_processes=2)(2, 2, 2)
    except ValueError:
        failed = True

    assert failed
