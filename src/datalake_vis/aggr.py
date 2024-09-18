""" Aggregate Enum and Aggregate Functions (Default and user-define) """

from abc import ABC, abstractmethod
from enum import Enum


class AggrFunction(ABC):
    """Abstract class for aggregate functions.
    To define customized aggregate, define the following:
    `name`: name of the aggregate
    `built_in`: True only if it is built-in in DataFrame, need to align with name
    `aggregate`: the aggregate function used to calculate aggregate result
    """

    @staticmethod
    @abstractmethod
    def aggregate(series):
        """Aggregate function to be defined

        Args:
            series: a pandas Series
        Returns:
            res: a concrete result of the aggregation of the data in series
        """
        raise NotImplementedError("Not implemented")


class Count(AggrFunction):
    """Built-in COUNT function"""

    name = "count"
    built_in = True

    @staticmethod
    def aggregate(series):
        return 0


class Sum(AggrFunction):
    """Built-in COUNT function"""

    name = "sum"
    built_in = True

    @staticmethod
    def aggregate(series):
        return 0


class Average(AggrFunction):
    """Built-in AVG function"""

    name = "mean"
    built_in = True

    @staticmethod
    def aggregate(series):
        return 0


class Min(AggrFunction):
    """Built-in MIN function"""

    name = "min"
    built_in = True

    @staticmethod
    def aggregate(series):
        return 0


class Max(AggrFunction):
    """Built-in MAX function"""

    name = "max"
    built_in = True

    @staticmethod
    def aggregate(series):
        return 0


class Aggregate(Enum):
    """Define all aggregate functions"""

    COUNT = Count
    SUM = Sum
    AVG = Average
    MIN = Min
    MAX = Max
