from workercontext import MultiWorkerAsync
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


def test_workers_no_wait(short_list, long_list):
    with MultiWorkerAsync(func_no_kwargs, 4) as f:
        f(short_list, 1, 1)

    with MultiWorkerAsync(func_no_kwargs, 10) as f:
        f(long_list, 1, 1)

    assert True


def test_workers_wait(short_list, long_list):
    res = []
    with MultiWorkerAsync(func_no_kwargs, 4, return_container=res) as f:
        f(short_list, 1, 1)
    assert res[0][0] == 3 and res[-1][-1] == 10
    assert sum(map(len, res)) == len(short_list)

    res = []
    with MultiWorkerAsync(func_no_kwargs, 10, return_container=res) as f:
        f(long_list, 1, 1)
    assert res[0][0] == 2 and res[-1][-1] == 1_000_001
    assert sum(map(len, res)) == len(long_list)
