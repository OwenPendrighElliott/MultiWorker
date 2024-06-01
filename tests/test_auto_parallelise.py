from workercontext import auto_parallelise
import multiprocessing
from workercontext.reductions import flatten_reduction
import pytest
from typing import List, Set


def list_inc_func(list_arg: List[int]) -> List[int]:
    return [x + 1 for x in list_arg]


def set_inc_func(list_arg: List[int]) -> Set[int]:
    return {x + 1 for x in list_arg}


def str_inc_func(list_arg: List[int]) -> str:
    return "".join([str(x + 1) for x in list_arg])


def unknown_reduction_return_func(list_arg: List[int]) -> int:
    return sum(list_arg)


def test_auto_parallel_basic_list():
    test_list = [1, 2, 3, 4, 5, 6, 7, 8]
    results = auto_parallelise(list_inc_func)(test_list)
    assert results == [2, 3, 4, 5, 6, 7, 8, 9]
    assert len(results) == len(test_list)


def test_auto_parallel_basic_set():
    test_list = [1, 1, 1, 2, 3, 4, 5, 6, 7, 8]
    results = auto_parallelise(set_inc_func)(test_list)
    assert results - {2, 3, 4, 5, 6, 7, 8, 9} == set()
    assert len(results) == len(test_list) - 2


def test_auto_parallel_basic_str():
    test_list = [1, 2, 3, 4, 5, 6, 7, 8]
    results = auto_parallelise(str_inc_func)(test_list)
    assert results == "23456789"
    assert len(results) == len(test_list)


def test_error_no_return():
    def no_return_func(list_arg: List[int]):
        return [x + 1 for x in list_arg]

    with pytest.raises(ValueError):
        auto_parallelise(no_return_func)


def test_error_no_batchable_arg():
    def no_batchable_arg_func(other_arg: int):
        return [x + 1 for x in range(other_arg)]

    with pytest.raises(ValueError):
        auto_parallelise(no_batchable_arg_func)


def test_error_unknown_reduction():
    """Need at least 2 cores to test this"""
    test_list = [1, 2]
    results = auto_parallelise(unknown_reduction_return_func)(test_list)
    if multiprocessing.cpu_count() > 1:
        assert len(results) == 2
