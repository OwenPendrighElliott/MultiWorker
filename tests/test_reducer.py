from multiworker import MultiWorker, flatten_list
import pytest
from typing import List


@pytest.fixture
def short_list():
    return [1, 2, 3, 4, 5, 6, 7, 8]


def list_return(l: List[int]) -> List[int]:
    return [el * 2 for el in l]


def int_return(l: List[int]) -> List[int]:
    return sum(l)


def test_flatten(short_list):
    with MultiWorker(list_return, 4, reducer=flatten_list) as f:
        res = f(short_list)
    assert res[0] == 2 and res[-1] == 16
    assert len(res) == len(short_list)


def test_sum(short_list):
    with MultiWorker(int_return, 4, reducer=sum) as f:
        res = f(short_list)
    assert res == sum(short_list)
