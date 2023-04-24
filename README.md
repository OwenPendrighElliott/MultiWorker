# Multi-Worker Context

This is a small project that create a context in python which can spin up multiple workers to do a task.

The only requirement is that a the function you want to give multiple workers to has only one argument that you would like to batch on. It is up to you to ensure that creating independant batches for multiple workers makes sense with this argument.

This makes refactoring code for multiprocessing a lot easier and should provide quick performance wins for time consuming functions with no interdependencies between elements.

## Setup

```
pip install -e .
```

## Example

By default the work distribution will occur on the first parameter to the function.

Take the following function:

```python
from typing import List
def my_func(arr: List[int]) -> int:
    ans = 0
    for el in arr:
        for i in range(10):
            ans += el*10
    return ans
```

This can be simply split across multiple workers as follows:

```python
from multiworker import MultiWorker

arr = list(range(100))

with MultiWorker(my_func, n_processes=8) as f:
    res = f(arr)
print(res)
```

`res` will be a list of `ints` in this case, if you would like to reduce across all workers then you can pass a reducer:

```python
from multiworker import MultiWorker

arr = list(range(100))

with MultiWorker(my_func, n_processes=8, reducer=sum) as f:
    res = f(arr)
print(res)
```

`res` will now be an `int`.

The library also includes a reducer for flattening lists:

```python
from multiworker import MultiWorker, flatten_list

def my_func(arr: List[int]) -> List[int]:
    for i in range(len(arr)):
        arr[i] += 1
    return arr

arr = list(range(100))

with MultiWorker(my_func, n_processes=8, reducer=flatten_list) as f:
    res = f(arr)
print(res)
```

### Using other parameters

You can batch work on other parameters by specifying them in the constructor.

```python
from multiworker import MultiWorker, flatten_list

def my_func(l1: List[int], l2: List[int]) -> int:
    for i in range(len(l2)):
        for el1 in l1:
            l2[i] += el1
    return l2

arr1 = list(range(100))
arr2 = list(range(100))

with MultiWorker(my_func, batched_arg='l2', n_processes=8, reducer=flatten_list) as f:
    res = f(arr1, arr2)
print(res)
```