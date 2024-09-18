""" Column Class """

import numpy as np
import pandas as pd
from dateutil.parser import parse
from scipy.stats import chi2_contingency, ks_2samp

from datalake_vis.data_types import DataType
from datalake_vis.text_converter import Word2Num, Word2Vec

# pylint: disable=W0718


class Column:
    """Column Class"""

    def __init__(
        self, table_name: str, column_name: str, table_id: int, column_index: int, column_df: pd.DataFrame
    ) -> None:
        self.table_name: str = table_name
        self.table_id: int = table_id
        self.column_name: str = column_name
        self.column_index: int = column_index
        self.data: pd.Series = column_df
        self.backup_data: pd.Series = []
        self.type: DataType = DataType.check_type(self.data)
        self.distribution = None
        self.is_date: bool = None
        self.is_valid: bool = self.data.isnull().all()

        self.metadata = {}

    def __str__(self) -> str:
        return f"Column: {self.column_name} | Type: {self.type} | Table: {self.table_name}\n"

    def __repr__(self) -> str:
        return f"Column: {self.column_name} | Type: {self.type} | Table: {self.table_name}\n"

    def prepare_column(self, is_preprocessed: bool, orig_data_path: str = None, text_format: str = "numerical"):
        if is_preprocessed:
            self._prepare_preprocessed_data(orig_data_path)
        else:
            self._prepare_fresh_data(text_format)
        self._construct_distribution()

    def _prepare_preprocessed_data(self, orig_data_path: str):
        tp = self.column_name.split("@")[-1]
        ty = tp.split(",")[0].strip("(").strip(" ")
        if ty == "textual":
            # need backup data for compute overlap, paths are hardcoded for now
            # path = "data/santos/query/..." if it is query else "data/santos/datalake/..."
            self.backup_data = pd.read_csv(f"{orig_data_path}", usecols=[self.column_index], low_memory=True).iloc[:, 0]

        self.type = (
            DataType.CATEGORICAL
            if ty == "categorical"
            else DataType.NUMERICAL if ty == "numerical" else DataType.TEXTUAL
        )
        self.is_date = True if tp.split(",")[1].strip(")").strip(" ") == "True" else False
        if self.data.isnull().all():
            return
        self.data.fillna(self.data.mode()[0], inplace=True)

    def _prepare_fresh_data(self, text_format: str = "numerical"):
        """Clean up NaN"""
        # all cell NaN, no hope to repair, ignore this
        if self.data.isnull().all():
            return

        # ad-hoc repairs
        self.data.replace("-", np.NaN, inplace=True)
        self.data.replace("Nil return", np.NaN, inplace=True)
        if self.data.isnull().all():
            return
        self.data.fillna(self.data.mode()[0], inplace=True)

        # if date, convert to int, this is invertible
        if self.is_date:
            last = None
            for idx in self.data.index:
                # print(self.data[idx])
                try:
                    self.data.at[idx] = parse(self.data[idx]).toordinal()
                    last = self.data[idx]
                except Exception:
                    self.data.at[idx] = last

        # if textual data, convert to numerical
        if self.type == DataType.TEXTUAL:
            if text_format == "vector":
                Word2Vec.convert(self.data)
            elif text_format == "numerical":
                Word2Num.convert(self.data)
            else:
                raise ValueError("Textual column can only be vector or number")
        return

    def _construct_distribution(self):
        if self.type == DataType.CATEGORICAL:
            self.distribution = self.data.value_counts().to_dict()
        elif self.type == DataType.NUMERICAL:
            try:
                self.distribution = self.data.sample(500).to_list()
            except ValueError:
                self.distribution = self.data.to_list()
        else:  # textual
            try:
                self.distribution = self.data.sample(500).to_list()
            except ValueError:
                self.distribution = self.data.to_list()
        return

    def is_mergeable_with(self, col: "Column") -> bool:
        """Statistically decide whether two columns can be merged

        Args:
            col: a Column
        Returns:
            a boolean indicates whether two columns can be merged
        """
        p = 0
        try:
            if self.type == DataType.CATEGORICAL:
                all_keys = set(list(self.distribution.keys()) + list(col.distribution.keys()))
                dist = []
                for key in all_keys:
                    cat_dist = []
                    if key in self.distribution:
                        cat_dist.append(self.distribution[key])
                    else:
                        cat_dist.append(0)
                    if key in col.distribution:
                        cat_dist.append(col.distribution[key])
                    else:
                        cat_dist.append(0)
                    dist.append(cat_dist)
                p = chi2_contingency(dist).pvalue
            else:  # numerical or textual, textual is converted to numerical already
                # print(self.distribution, col.distribution)
                p = ks_2samp(self.distribution, col.distribution).pvalue
        except Exception:
            # test failed, don't merge
            return False
        if p > 0.05:
            return True
        return False

    def check_overlap(self, col: "Column") -> float:
        """Check data overlap between two columns

        Args:
            col: a Column
        Return:
            a number range [0,1] indicating overlap between self and col
        """
        if self.type == DataType.NUMERICAL:
            # range overlap percentage, pick the max
            # date can be treated as numerical
            overlap_range = min(self.data.max(), col.data.max()) - max(self.data.min(), col.data.min())
            return max(
                overlap_range / (self.data.max() - self.data.min()), overlap_range / (col.data.max() - col.data.min())
            )
        elif self.type == DataType.CATEGORICAL:
            # Jaccard distance between sets of categories
            col1_uniq = set(self.data.unique())
            col2_uniq = set(self.data.unique())
            return len(col1_uniq.intersection(col2_uniq)) / len(col1_uniq.union(col2_uniq))
        # TEXTUAL: Jaccard distance between sets of words
        # note str() conversion is necessary to safe-guard the split
        col1_wordset = set((word for cell in self.backup_data for word in str(cell).split()))
        col2_wordset = set((word for cell in col.backup_data for word in str(cell).split()))
        return len(col1_wordset.intersection(col2_wordset)) / len(col1_wordset.union(col2_wordset))
