""" Miscellaneous helper functions """

import functools
import math
import time
from itertools import combinations
from typing import Dict, List, Tuple

import pandas as pd
from dateutil.parser import parse
from matplotlib import pyplot as plt
import json
import numpy as np

# pylint: disable=W0718


def confidence_interval(m: int, n: int, err: float = 0.05):
    """Hoeffding-Sterfling Inequality"""
    # return math.sqrt((1 - (m - 1) / n) / 2 / m * (2 * math.log(math.log(m)) + math.log(math.pi**2 / 3 / err)))
    return math.sqrt(math.log(err / 2) * -1 * (1 - (m - 1) / (n - 1)) / m / 2 * 0.25**2)


def find_all_pairs(elements: List):
    """Find all combinations of 2 elements in a list"""
    return list(combinations(elements, 2))


def is_date(col: pd.Series, fuzzy=False):
    """Return whether the string can be interpreted as a date.

    Args
        string: str, string to check for date
        fuzzy: bool, ignore unknown tokens in string if True
    Return
        bool indicating if string is date format
    """
    if col is None or col.empty:
        return False

    try:
        int(col.iloc[0])
        return False
    except Exception:
        pass

    try:
        float(col.iloc[0])
        return False
    except Exception:
        pass

    try:
        parse(str(col.iloc[0]), fuzzy=fuzzy)
        return True
    except ValueError:
        return False


def find_bucket(buckets: List[Tuple], num) -> int:
    """Binary search to find the right buckets

    Args
        buckets: a list of intervals
        num: a number (int, float, etc.)
    Return
        the bucket index (start, end] that start < num < end
    """
    left, right = 0, len(buckets) - 1

    while left <= right:
        mid = (left + right) // 2
        start, end = buckets[mid]

        if start < num <= end:
            return mid
        elif num < start:
            right = mid - 1
        else:
            left = mid + 1

    return -1


def runtime_logger(func):
    """Function decorator that measure and record runtime of class function"""

    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        start_time = time.time()
        result = func(self, *args, **kwargs)
        end_time = time.time()
        runtime = end_time - start_time

        # Log the runtime in the class variable
        if not hasattr(self, "runtimes"):
            self.runtimes = {}
        self.runtimes[func.__name__] = runtime

        # print(f"Function {func.__name__} took {runtime:.6f} seconds to complete")
        return result

    return wrapper


def plot_vis_plan(data: Dict[str, List[float]], x_name: str, y_name: str, aggr: str, path: str, series: List[str]):
    """Plot and save figure"""
    tp = {}
    for v in data.values():
        for i, e in enumerate(v):
            if series[i] not in tp:
                tp[series[i]] = []
            tp[series[i]].append(e)

    index = data.keys()
    try:
        df = pd.DataFrame(tp, index=index)
    except Exception as e:
        print(e)
        print(x_name, y_name, aggr)
        print(data)
    ax = df.plot.bar(rot=45)
    ax.plot()
    plt.xlabel(x_name)
    plt.ylabel(y_name)
    plt.tight_layout()
    plt.savefig(f"{path}/{x_name}_{y_name}_{aggr}.png")
    plt.clf()
    plt.close()


class NumpyEncoder(json.JSONEncoder):
    """Custom encoder for numpy data types"""

    def default(self, obj):
        if isinstance(
            obj,
            (
                np.int_,
                np.intc,
                np.intp,
                np.int8,
                np.int16,
                np.int32,
                np.int64,
                np.uint8,
                np.uint16,
                np.uint32,
                np.uint64,
            ),
        ):

            return int(obj)

        elif isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)

        elif isinstance(obj, (np.complex_, np.complex64, np.complex128)):
            return {"real": obj.real, "imag": obj.imag}

        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()

        elif isinstance(obj, (np.bool_)):
            return bool(obj)

        elif isinstance(obj, (np.void)):
            return None

        return json.JSONEncoder.default(self, obj)
