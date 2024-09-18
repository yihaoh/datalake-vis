""" Series class for combined columns """

from typing import List

import pandas as pd

from datalake_vis.column import Column
from datalake_vis.data_types import DataType


class Series:
    """Series class for combined columns"""

    def __init__(self, columns: List[Column], name: str) -> None:
        self.columns: List[Column] = columns
        self.name: str = name
        self.type: DataType = columns[0].type
        self.is_date: bool = columns[0].is_date
        self.valid: bool = all(not c.is_valid for c in self.columns)
        self.data: pd.Series = (
            pd.concat([c.data for c in self.columns], axis=0) if len(columns) > 1 else columns[0].data
        )

    def combine_data(self) -> pd.DataFrame:
        """return combined data from columns"""
        return self.data

    def __str__(self) -> str:
        return "(" + " , ".join([f"{col.table_name}({col.table_id}).{col.column_name}" for col in self.columns]) + ")"

    def __repr__(self) -> str:
        return " , ".join([f"{col.table_name}.{col.column_name}" for col in self.columns])
