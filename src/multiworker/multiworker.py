import multiprocessing
from functools import partial
import inspect
from typing import List, Any, Callable


def _worker(kwargs, function: Callable) -> Any:
    """Helper to wrap a function for multiprocessing"""
    return function(**kwargs)


def flatten_list(lst: List[List[Any]]) -> List[Any]:
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


class MultiWorker:
    def __init__(
        self,
        function: Callable,
        n_processes: int,
        batched_arg: str = None,
        verbose: bool = False,
        reducer: Callable[[List[Any]], Any] = None,
    ):
        """Create a multiprocessing context for functions

        Args:
            function (Callable): The function to create the context for.
            n_processes (int): The number of processes to spawn.
            batched_arg (str, optional): The argument to batch on, if None the the first arg is used. Defaults to None.
            verbose (bool, optional): Whether or not to print information about the processing. Defaults to False.
            reducer (Callable[[List[Any]], Any], optional): A reducer function to be applied across the outputs of the pool. Defaults to None.
        """
        self.function = function
        self.batched_arg = batched_arg
        self.n_processes = n_processes
        self._verbose = verbose
        self._reducer = reducer
        self.pool = multiprocessing.Pool(self.n_processes)

    def batchify(self, lst, chunk_size):
        return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]

    def _batched_function(self, *args, **kwargs) -> List[Any]:
        """Wrap any function in this batched version, converts all args into kwargs using introspection

        Raises:
            ValueError: If the batched args are not a list

        Returns:
            List[Any]: A list of outputs from each batch, ordered.
        """

        # Introspect the args to the function
        argspec = inspect.getfullargspec(self.function)
        func_args = argspec.args

        # if the first arg is self then drop it because it is provided by its class
        if func_args[0] == "self":
            func_args = func_args[1:]

        # If there is not batched arg then use the first
        if not self.batched_arg:
            self.batched_arg = func_args[0]

        if self._verbose:
            print(
                f"Executing {self.function.__name__} with {self.n_processes} workers evenly distributing work on parameter '{self.batched_arg}'"
            )

        # convert all args to kwargs using introspected args
        for i, arg in enumerate(args):
            kwargs[func_args[i]] = arg

        if not isinstance(kwargs[self.batched_arg], list):
            raise ValueError("Batched args must be an instance of list")

        # get size of batched arg
        arg_size = len(kwargs[self.batched_arg])

        # calculate chunksize
        chunk_size = arg_size // self.n_processes

        if self._verbose:
            print(f"Workers will be assigned chunks of size {chunk_size}")

        # convert the batched arg into batches
        kwargs[self.batched_arg] = self.batchify(
            kwargs[self.batched_arg], chunk_size=chunk_size
        )

        # create a list of kwargs with each batched_arg arg having a single batch
        batched_args = []
        for batch in kwargs[self.batched_arg]:
            new_args = dict(kwargs)
            new_args[self.batched_arg] = batch
            batched_args.append(new_args)

        # create a pool - the function is wrapped up in a worker so we can pool on all args
        func_outputs = self.pool.map(
            partial(_worker, function=self.function), batched_args
        )

        if self._reducer:
            func_outputs = self._reducer(func_outputs)

        return func_outputs

    def __enter__(self) -> Callable:
        """Creates a temporary multiprocessing context for a function

        Returns:
            Callable: Return a batched version of the function with multiprocessing
        """
        return self._batched_function

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clone the pool when done"""
        self.pool.close()
        self.pool.join()
