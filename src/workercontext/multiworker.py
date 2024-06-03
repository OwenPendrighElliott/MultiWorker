import multiprocessing
from functools import partial
import inspect
from .reductions import (
    flatten_reduction,
    set_union_reduction,
    dict_merge_reduction,
    string_concatenation_reduction,
)
from typing import List, Any, Callable, Union


def _worker(kwargs, function: Callable) -> Any:
    """Helper to wrap a function for multiprocessing"""
    return function(**kwargs)


class MultiWorker:
    def __init__(
        self,
        function: Callable,
        n_processes: int,
        batched_arg: str = None,
        verbose: bool = False,
        reduction: Callable[[List[Any]], Any] = None,
    ):
        """Create a multiprocessing context for functions

        Args:
            function (Callable): The function to create the context for.
            n_processes (int): The number of processes to spawn.
            batched_arg (str, optional): The argument to batch on, if None the the first arg is used. Defaults to None.
            verbose (bool, optional): Whether or not to print information about the processing. Defaults to False.
            reduction (Callable[[List[Any]], Any], optional): A reduction function to be applied across the outputs of the pool. Defaults to None.
        """
        self.function = function
        self.batched_arg = batched_arg
        self.n_processes = n_processes
        self._verbose = verbose
        self._reduction = reduction
        self.pool = multiprocessing.Pool(self.n_processes)

    def batchify(self, lst, chunk_size):
        return [lst[i : i + chunk_size] for i in range(0, len(lst), chunk_size)]

    def _batched_args(self, *args, **kwargs) -> List[Any]:
        """Wrap any function in this batched version, converts all args into kwargs using introspection

        Raises:
            ValueError: If the batched args are not a list

        Returns:
            List[Any]: A list of outputs from each batch, ordered.
        """

        # Introspect the args to the function
        argspec = inspect.getfullargspec(self.function)
        func_args = argspec.args

        annotations = argspec.annotations

        # if the first arg is self then drop it because it is provided by its class
        if func_args[0] == "self":
            func_args = func_args[1:]

        # if no batched arg is specified then try to determine it from the type hints or fall back to the first arg
        if not self.batched_arg:
            all_typed = all([arg in annotations for arg in func_args])
            if not all_typed:
                if self._verbose:
                    print(
                        f"No batched arg specified and not all args are typed, defaulting to first arg: {func_args[0]}"
                    )
                self.batched_arg = func_args[0]
            else:
                for arg in func_args:
                    arg_type = annotations[arg]
                    if isinstance(arg_type, list) or (
                        hasattr(arg_type, "__origin__") and arg_type.__origin__ == list
                    ):
                        self.batched_arg = arg
                        if self._verbose:
                            print(
                                f"Using argument type hints, automatically determined batched arg: {arg}"
                            )
                        break

                # if we still don't have a batched arg then raise an error
                if not self.batched_arg:
                    raise ValueError(
                        "No batched arg specified and no args are typed as lists, please specify a batched arg or check your type hints are correct"
                    )

        # convert all args to kwargs using introspected args
        for i, arg in enumerate(args):
            kwargs[func_args[i]] = arg

        if not isinstance(kwargs[self.batched_arg], list):
            raise ValueError(
                f"Batched arg {self.batched_arg} must be an instance of list"
            )

        # get size of batched arg
        arg_size = len(kwargs[self.batched_arg])

        processes_to_use = min(self.n_processes, arg_size)

        if self._verbose:
            print(
                f"Executing {self.function.__name__} with {processes_to_use} workers evenly distributing work on parameter '{self.batched_arg}'"
            )

        # calculate chunksize
        chunk_size = arg_size // processes_to_use

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
        return batched_args

    def _batched_function(self, *args, **kwargs) -> List[Any]:
        batched_args = self._batched_args(*args, **kwargs)

        # create a pool - the function is wrapped up in a worker so we can pool on all args
        func_outputs = self.pool.map(
            partial(_worker, function=self.function), batched_args
        )

        if self._reduction:
            func_outputs = self._reduction(func_outputs)

        return func_outputs

    def __enter__(self) -> Callable:
        """Creates a temporary multiprocessing context for a function

        Returns:
            Callable: Return a batched version of the function with multiprocessing
        """
        return self._batched_function

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the pool when done"""
        self.pool.close()
        self.pool.join()


class MultiWorkerAsync(MultiWorker):
    """
    A special case of the MultiWorker that uses map_async instead of map to allow for async processing of the pool

    This allows for async processing within the context and a gaurentee that at the end the end of the context
    results will populate the return container
    """

    def __init__(
        self,
        function: Callable,
        n_processes: int,
        batched_arg: str = None,
        verbose: bool = False,
        return_container: list = None,
    ):
        super().__init__(function, n_processes, batched_arg, verbose, None)

        self.func_outputs = None
        self.return_container = return_container

    def _batched_function(self, *args, **kwargs) -> None:
        batched_args = self._batched_args(*args, **kwargs)

        self.func_outputs = self.pool.map_async(
            partial(_worker, function=self.function), batched_args
        )

    def __enter__(self) -> Callable:
        """Creates a temporary multiprocessing context for a function

        Returns:
            Callable: Return a batched version of the function with multiprocessing
        """
        return self._batched_function

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Close the pool when done and populate the return container if it is provided"""
        results = self.func_outputs.get()
        if self.return_container is not None:
            for result in results:
                self.return_container.append(result)

        self.pool.close()
        self.pool.join()


def parallelise(
    function: Callable,
    n_processes: int,
    batched_arg: str = None,
    verbose: bool = False,
    reduction: Callable[[List[Any]], Any] = None,
) -> Callable:
    """Parallelise a function without a context manager

    Args:
        function (Callable): The function to parallelise
        n_processes (int): The number of processes to use
        batched_arg (str, optional): The argument to batch on. Defaults to None.
        verbose (bool, optional): Toggle verbose printing. Defaults to False.
        reduction (Callable[[List[Any]], Any], optional): Reduction to apply to the output. Defaults to None.

    Returns:
        Callable: A parallelised function
    """
    return MultiWorker(
        function, n_processes, batched_arg, verbose, reduction
    ).__enter__()


def auto_parallelise(
    function: Callable,
) -> Callable:
    """Automatically parallelise a function based on type hints. Works like
    parallelise but automatically determines the return type and applies a reduction if possible.

    Args:
        function (Callable): The function to parallelise

    Raises:
        ValueError: raised if the function has no type hints
        ValueError: raised if the function has no return type hint

    Returns:
        Callable: a parallelised function
    """

    # automatically use the same number of processes as the number of cpus
    n_processes = multiprocessing.cpu_count()

    # introspect the function to determine the return type
    argspec = inspect.getfullargspec(function)
    if not argspec.annotations:
        raise ValueError("Function must have type hints to use auto_parallelise")

    annotations = argspec.annotations

    if "return" not in annotations:
        raise ValueError(
            "Function must have a return type hint to use auto_parallelise"
        )

    return_type = annotations["return"]
    print(f"Return type is annotated as {return_type}")

    return_module = return_type.__module__

    if return_module == "typing":
        return_type = return_type.__origin__

    reduction = None
    if return_type == list:
        reduction = flatten_reduction
        print(
            "Lists from each worker will be flattened into a single list with order preserved"
        )
    elif return_type == set:
        reduction = set_union_reduction
        print("Sets from each worker will be unioned into a single set")
    elif return_type == dict:
        reduction = dict_merge_reduction
        print("Dictionaries from each worker will be merged into a single dictionary")
    elif return_type == str:
        reduction = string_concatenation_reduction
        print(
            "Strings from each worker will be concatenated into a single string with order preserved"
        )
    else:
        print(
            f"No reduction will be applied to the return type as we don't automatically know a smart thing to do for {return_type}"
        )

    return parallelise(function, n_processes, verbose=True, reduction=reduction)


def parallelise_async(
    function: Callable, n_processes: int, batched_arg: str = None, verbose: bool = False
) -> Callable:
    """Create an async parallelised function

    Args:
        function (Callable): The function to parallelise
        n_processes (int): The number of processes to use
        batched_arg (str, optional): The argument to batch on. Defaults to None.
        verbose (bool, optional): Toggle verbose logging. Defaults to False.

    Returns:
        Callable: A parallelised function
    """
    return MultiWorkerAsync(function, n_processes, batched_arg, verbose).__enter__()
