from typing import List, Dict, Union, Callable, Any


class ReductionComposition:
    """Compose multiple reductions into a pipeline"""

    def __init__(self, reductions: List[Callable] = []):
        self._reductions = reductions

    def __call__(self, arg: Any) -> Any:
        """Execute the reduction pipeline

        Args:
            arg (Any): The arg to reduce.

        Returns:
            Any: The output of the last reduction.
        """
        for reduction in self._reductions:
            print(arg)
            arg = reduction(arg)
        return arg


def flatten_reduction(lst: List[List[Any]]) -> List[Any]:
    """
    Flatten a list of lists into a single list.

    Args:
        lst (List[List[Any]]): A list of lists.

    Returns:
        List[Any]: A flattened list with the same ordering.
    """
    flattened = []
    for sublist in lst:
        flattened.extend(sublist)
    return flattened


def sum_reduction(arr: List[float]) -> float:
    """
    Returns the sum of all the elements in the input array.
    
    Args:
        arr (List[float]): A list of numbers.
        
    Returns:
        float: The sum of all the elements in the input array.
    """
    result = 0
    for elem in arr:
        result += elem
    return result


def average_reduction(lst: List[float]) -> float:
    """
    Average a list of numbers

    Args:
        lst (List[float]): A list of floats.

    Returns:
        float: Average.
    """
    return sum(lst) / len(lst)


def histogram_reduction(arr: List[Union[int, str]]) -> Dict[Union[int, str], int]:
    """Returns a dictionary containing the count of each unique element in the input array.
    
    Args:
        arr (List[int]): A list of integers.
        
    Returns:
        Dict[int, int]: A dictionary where keys are unique elements from the input array and values are their counts.
    """
    histogram = {}
    for elem in arr:
        if elem not in histogram:
            histogram[elem] = 1
        else:
            histogram[elem] += 1
    return histogram


def product_reduction(arr: List[float]) -> float:
    """Returns the product of all the elements in the input array.
    
    Args:
        arr (List[float]): A list of numbers.
        
    Returns:
        float: The product of all the elements in the input array.
    """
    result = 1
    for elem in arr:
        result *= elem
    return result


def string_concatenation_reduction(arr: List[str]) -> str:
    """Returns the concatenation of all the strings in the input array.
    
    Args:
        arr (List[str]): A list of strings.
        
    Returns:
        str: The concatenation of all the strings in the input array.
    """
    return "".join(arr)


def bitwise_and_reduction(arr: List[int]) -> int:
    """Returns the bitwise AND of all the integers in the input array.
    
    Args:
        arr (List[int]): A list of integers.
        
    Returns:
        int: The bitwise AND of all the integers in the input array.
    """
    result = arr[0]
    for elem in arr[1:]:
        result &= elem
    return result


def bitwise_or_reduction(arr: List[int]) -> int:
    """Returns the bitwise OR of all the integers in the input array.
    
    Args:
        arr (List[int]): A list of integers.
        
    Returns:
        int: The bitwise OR of all the integers in the input array.
    """
    result = arr[0]
    for elem in arr[1:]:
        result |= elem
    return result


def min_reduction(arr: List[float]) -> float:
    """Returns the minimum of a result set.

    Args:
        arr (List[float]): A list of numbers.

    Returns:
        float: The mminimum.
    """

    return min(arr)


def max_reduction(arr: List[float]) -> float:
    """Returns the minimum of a result set.

    Args:
        arr (List[float]): A list of numbers.

    Returns:
        float: The mminimum.
    """

    return max(arr)
