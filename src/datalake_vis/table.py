""" Table class """

from typing import List

import pandas as pd

from datalake_vis.column import Column


class Table:
    """Table Class"""

    def __init__(self, table_name: str, table_id: int, table_df: pd.DataFrame) -> None:
        self.name: str = table_name
        self.id: int = table_id  # note that id 0 always indicate query table
        self.data: pd.DataFrame = table_df
        self.columns: List[Column] = []
        self._clean_data()
        for i, col_name in enumerate(table_df):
            self.columns.append(Column(table_name, col_name, table_id, i, table_df[col_name]))

    def prepare_table(self, is_preprocessed: bool, orig_data_path: str = None, text_format: str = "numerical"):
        for col in self.columns:
            col.prepare_column(is_preprocessed, orig_data_path, text_format)

    def size(self):
        """return size"""
        return self.data.shape

    def _clean_data(self):
        """Table-level cleanup"""
        # self.data.dropna(inplace=True)
        self.data.rename(columns=lambda name: name.replace(" ", "_").replace("/", "__"), inplace=True)

    def __getitem__(self, i) -> Column:
        return self.columns[i]
