# Multi-Worker Context

This is a small project that create a context in python which can spin up multiple workers to do a task.

The only requirement is that a the function you want to give multiple workers to has only one argument that you would like to batch on. It is up to you to ensure that creating independant batches for multiple workers makes sense with this argument.

This makes refactoring code for multiprocessing a lot easier and should provide quick performance wins for time consuming functions with no interdependencies between elements.

## Setup

```
pip install workercontext
```

## Example

By default the work distribution will occur on the first parameter to the function.

Take the following function:

```python
from typing import List

def my_func(arr: List[int]) -> List[int]:
    return [el*2 for el in arr]
```

This can be simply split across multiple workers as follows:

```python
from workercontext import workercontext

arr = list(range(100))

with workercontext(my_func, n_processes=8) as f:
    res = f(arr)
print(res)
```

`res` will be a list of lists of `ints` in this case, if you would like to reduce across all workers then you can pass a reduction:

```python
from workercontext import workercontext
from workercontext.reductions import flatten_reduction

arr = list(range(100))

with workercontext(my_func, n_processes=8, reduction=flatten_reduction) as f:
    res = f(arr)
print(res)
```

`res` will now a list of `int`.

If you wanted to combine multiple reductions then you can use the reduction composition class

```python
from workercontext import workercontext
from workercontext.reductions import ReductionComposition, flatten_reduction, average_reduction

reductions = ReductionComposition([flatten_reduction, average_reduction])

with workercontext(my_func, n_processes=8, reduction=reductions) as f:
    res = f(arr)
print(res)
```

This makes res be a single `float`.

### Using other parameters

You can batch work on other parameters by specifying them in the constructor.

```python
from workercontext import workercontext
from workercontext.reductions import flatten_reduction

def my_func(l1: List[int], l2: List[int]) -> int:
    for i in range(len(l2)):
        for el1 in l1:
            l2[i] += el1
    return l2

arr1 = list(range(100))
arr2 = list(range(100))

with workercontext(my_func, batched_arg='l2', n_processes=8, reduction=flatten_reduction) as f:
    res = f(arr1, arr2)
print(res)
```

# Documentation

## workercontext
### parameters
+ `function` (`Callable`): The function to create the context for.
+ `n_processes` (`int`): The number of processes to spawn.
+ `batched_arg` (`str`, `optional`): The argument to batch on, if None the the first arg is used. Defaults to None.
+ `verbose` (`bool`, `optional`): Whether or not to print information about the processing. Defaults to False.
+ `reduction` (`Callable[[List[Any]], Any]`, `optional`): A reduction function to be applied across the outputs of the pool. Defaults to None.
## Supported Reductions
+ `flatten_reduction`
+ `histogram_reduction`
+ `product_reduction`
+ `string concatenation_reduction`
+ `bitwise and_reduction`
+ `bitwise or_reduction`
+ `min_reduction`
+ `max_reduction`

## Testing
```
pytest
```

## Formatting
```
black .
```

## How it works

TL;DR it does a bunch of introspection.

1. The args to your function are introspected.
2. The `self` arg is remove if you passed it a method from a class.
3. If no batched arg was specified then the first one is selected.
4. All args are converted into kwargs using the introspected arg names and the `*args` provided.
5. The size of the batched arg is calculated and the chunk sizes are derived.
6. The arg is batched and batches of kwargs are created.
7. A pool is created with a partial for a wrapper function that allows for the batching to occur on the kwargs. The last parameter to the wrapper is a callback to your function.
8. A reduction is applied (if specified)
9. Results are returned.
10. When you leave the context the pools are joined and closed.