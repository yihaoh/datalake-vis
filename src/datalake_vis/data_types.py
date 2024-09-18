""" Various types definitions """

import math
from enum import Enum

import pandas as pd
from pandas.api.types import is_bool_dtype, is_numeric_dtype

from datalake_vis.utils import is_date


class DataType(Enum):
    """Define all possible data types"""

    CATEGORICAL = "categorical"
    NUMERICAL = "numerical"
    TEXTUAL = "textual"

    @staticmethod
    def check_type(col: pd.Series):
        """Return the type of the column data

        Args:
            col: a column dataframe
        Return:
            ty: a DataType in [numerical, categorical, textual]
        """
        if is_bool_dtype(col) or col.nunique() < math.log2(col.shape[0]):
            return DataType.CATEGORICAL
        elif is_numeric_dtype(col) or is_date(col):
            return DataType.NUMERICAL
        return DataType.TEXTUAL
