from workercontext import MultiWorker
from workercontext.reductions import (
    ReductionComposition,
    flatten_reduction,
    sum_reduction,
    average_reduction,
    histogram_reduction,
    product_reduction,
    string_concatenation_reduction,
    min_reduction,
    max_reduction,
)
import pytest
from typing import List


@pytest.fixture
def short_list():
    return [1, 2, 3, 4, 5, 6, 7, 8]


@pytest.fixture
def short_str_list():
    return ["Hello", "World", "I", "Am", "A", "String"]


@pytest.fixture
def duplicated_list():
    return [1, 1, 1, 1, 2, 2, 2, 2]


def list_return(l: List[int]) -> List[int]:
    return [el * 2 for el in l]


def concat(l: List[str]) -> str:
    return "".join(l)


def int_return(l: List[int]) -> List[int]:
    return sum(l)


def do_nothing(l: List[int]) -> List[int]:
    return l


def test_flatten(short_list):
    with MultiWorker(list_return, 4, reduction=flatten_reduction) as f:
        res = f(short_list)
    assert res[0] == 2 and res[-1] == 16
    assert len(res) == len(short_list)


def test_sum(short_list):
    with MultiWorker(int_return, 4, reduction=sum_reduction) as f:
        res = f(short_list)
    assert res == sum(short_list)


def test_average(short_list):
    reductions = ReductionComposition([flatten_reduction, average_reduction])
    with MultiWorker(do_nothing, 4, reduction=reductions) as f:
        res = f(short_list)
    assert res == sum(short_list) / len(short_list)


def test_histogram(duplicated_list):
    with MultiWorker(int_return, 4, reduction=histogram_reduction) as f:
        res = f(duplicated_list)
    print(res)
    assert res[2] == 2


def test_min(short_list):
    with MultiWorker(int_return, 4, reduction=min_reduction) as f:
        res = f(short_list)
    assert res == 3


def test_max(short_list):
    with MultiWorker(int_return, 4, reduction=max_reduction) as f:
        res = f(short_list)
    assert res == 15


def test_composed(short_list):
    reductions = ReductionComposition([flatten_reduction, product_reduction])

    with MultiWorker(list_return, 4, reduction=reductions) as f:
        res = f(short_list)
    assert res == 10321920


def test_string_concat(short_str_list):
    with MultiWorker(concat, 4, reduction=string_concatenation_reduction) as f:
        res = f(short_str_list)
    assert res == "".join(short_str_list)
