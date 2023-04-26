from workercontext import MultiWorker
import pytest
from typing import List, Union
import time


@pytest.fixture
def long_int_list():
    return list(range(10_000_000))


@pytest.fixture
def long_str_list():
    return [str(n) for n in range(10_000_000)]


def busy_func(l: List[Union[str, int]]):
    for el in l:
        for i in range(10):
            el *= i


def test_busy_spd(long_int_list, long_str_list):
    start_single = time.perf_counter()
    busy_func(long_int_list)
    end_single = time.perf_counter()

    total_single = end_single - start_single

    start_multi = time.perf_counter()
    with MultiWorker(busy_func, 8) as f:
        f(long_int_list)
    end_multi = time.perf_counter()

    total_multi = end_multi - start_multi

    print(total_multi, total_single)
    assert total_multi < total_single

    start_single = time.perf_counter()
    busy_func(long_str_list)
    end_single = time.perf_counter()

    total_single = end_single - start_single

    start_multi = time.perf_counter()
    with MultiWorker(busy_func, 8) as f:
        f(long_str_list)
    end_multi = time.perf_counter()

    total_multi = end_multi - start_multi

    print(total_multi, total_single)
    assert total_multi < total_single
