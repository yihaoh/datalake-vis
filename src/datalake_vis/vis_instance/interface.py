""" Visualization Instance Interface """

import math
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans

from datalake_vis.aggr import Aggregate
from datalake_vis.col_vis_plan import ColVisPlan
from datalake_vis.data_types import DataType
from datalake_vis.series import Series
from datalake_vis.table import Table
from datalake_vis.utils import find_bucket


class VisInstance(ABC):
    """VisInstance class to hold input information and govern the visualization process"""

    def __init__(self, **kwarg):
        self.query_table: Table = None
        self.result_tables: List[Table] = []
        self.name_to_tables: Dict[str, Table] = {}
        self.column_matchings: Dict[str, Dict[int, int]] = (
            {}
        )  # result table name -> jth column in query table -> kth column in ith result table
        self.bucket_num: int = 0
        self.k: int = 0
        self.top_k: List[ColVisPlan] = []
        self.preprocessed_data: bool = False
        self.text_format: str = ""

        mandatory_fields = [
            "query_path",
            "result_paths",
            "query_table_name",
            "result_table_names",
            "column_matchings",
            "bucket_num",
            "preprocessed_data",
            "orig_query_path",
            "orig_result_paths",
            "text_format",
            # "total_batch" is not mandatory, only needed for pruning
        ]

        if not all(key in kwarg for key in mandatory_fields):
            raise KeyError("Please provide all of the following: ", mandatory_fields)

        if "column_matchings" not in kwarg:
            raise KeyError("Must provide column_matchings: list[list[tuple[int, int]]] between query and all results.")

        self.bucket_num = kwarg["bucket_num"]
        self.preprocessed_data = kwarg["preprocessed_data"]
        self.text_format = kwarg["text_format"]

        self.query_table = Table(kwarg["query_table_name"], 0, pd.read_csv(kwarg["query_path"], low_memory=True))
        self.query_table.prepare_table(self.preprocessed_data, kwarg["orig_query_path"], self.text_format)
        self.result_tables = [
            Table(table_name, i + 1, pd.read_csv(path, low_memory=True))
            for i, (table_name, path) in enumerate(zip(kwarg["result_table_names"], kwarg["result_paths"]))
        ]
        for i, t in enumerate(self.result_tables):
            t.prepare_table(self.preprocessed_data, kwarg["orig_result_paths"][i], self.text_format)

        self.column_matchings = {
            table_name: {j: k for (j, k) in matching} for table_name, matching in kwarg["column_matchings"]
        }
        self.name_to_tables = {t.name: t for t in self.result_tables}

    def _compute_buckets(self, x: int, x_series: List[Series]) -> Tuple[List, Any]:
        """Given a column data in DataFrame, compute buckets (can be categories or ranges)

        Args:
            x: query table column index
            x_series: a list of x series
            bucket_num: number of buckets, only for numerical / textual data
        Return:
            buckets: a list of string if categorical data, otherwise a list of ranges
            bucket_locator: a callable function or object (e.g. KMeans) that can determine bucket
        """
        if self.query_table[x].type == DataType.CATEGORICAL:
            buckets = set()
            for xs in x_series:
                for c in xs.columns:
                    for key in c.data.unique():
                        buckets.add(str(key))
            return list(buckets), str
        if self.query_table[x].type == DataType.NUMERICAL or (
            self.query_table[x].type == DataType.TEXTUAL and self.text_format == "numerical"
        ):
            data_max = max((c.data.max() for xs in x_series for c in xs.columns))
            data_min = min((c.data.min() for xs in x_series for c in xs.columns))
            offset = (data_max - data_min) / self.bucket_num
            buckets = [
                (math.floor(data_min + i * offset), math.floor(data_min + (i + 1) * offset))
                for i in range(self.bucket_num)
            ]
            return buckets, find_bucket
        # textual as vector
        vectors = np.array(self.query_table[x].data.to_list())
        kmeans = KMeans(n_clusters=self.bucket_num, random_state=0)
        kmeans.fit(vectors)
        labels = np.unique(kmeans.labels_).tolist()
        return labels, kmeans

    @abstractmethod
    def compute_x_series(self, x: int) -> Tuple[List[Series], List, Any]:
        """Compute series s.t. each series contains only 1 column from result table.
        Discard the result table column if it is not type-compatible with x.

        Args
            x: index to the query table column
        Return
            x_series: a list of x series that will be used to compute aggregate results
            buckets: same as :func:`_compute_buckets`
            bucket_locator: same as :func:`_compute_buckets`
        """
        raise NotImplementedError("Please Implement this method")

    # def compute_y_series(self, x: int, x_series: "list[Series]") -> "dict[int, tuple[list[Series], list[Series]]]":
    # raise NotImplementedError("Please Implement this method")

    def compute_y_series(self, x: int, x_series: List[Series]) -> Dict[int, Tuple[List[Series], List[Series]]]:
        """Given a query table column x, and its x_series, compute all possible y_series. Rearrange x_series
        so that their corresponding y_series are also type-compatible.

        Args:
            x: index to the query table column that represents the x-axis
            x_series: a list of Series, for each we should generate corresponding group of y columns
        Returns:
            res: a dict, columns index (except x) in query table as y-axis -> a tuple of (x_series, y_series)
            with guarantee of both being type-compatible
        """
        res = {}
        if not x_series:
            return res
        x_name = x_series[0].name
        for i, query_col in enumerate(self.query_table.columns):
            if i == x or query_col.is_date:
                # skip note: 1) x can't be its own y, 2) y being date (numerical type) is not meaningful
                continue
            y_name = self.query_table[i].column_name
            new_x_series: List[Series] = []
            new_y_series: List[Series] = []
            for _, s in enumerate(x_series):
                # now examine all y columns, try our best to preserve the type-compatible ones
                cols = [
                    (
                        self.query_table[i]
                        if c.table_id == 0  # if query table
                        else (
                            self.name_to_tables[c.table_name][self.column_matchings[c.table_name][i]]
                            if i in self.column_matchings[c.table_name]  # i must match a column in result table
                            else None
                        )
                    )
                    for c in s.columns
                ]  # all y-series for x-series
                new_x_cols = []
                new_y_cols = []
                # print(new_x_cols, new_y_cols)
                for x_col, y_col in zip(s.columns, cols):
                    if y_col is not None and y_col.type == query_col.type and x_col.is_date == y_col.is_date:
                        new_x_cols.append(x_col)
                        new_y_cols.append(y_col)
                if not new_x_cols or not new_y_cols:  # no hope for this column, done
                    break
                # otherwise add the series
                new_x_series.append(Series(new_x_cols, x_name))
                new_y_series.append(Series(new_y_cols, y_name))
            if (
                new_x_series
                and new_y_series
                and sum((len(s.columns) for s in new_x_series)) >= 2  # loose filter
                # preserve 30% of the columns
                # and sum([len(s.columns) for s in new_x_series]) / sum([len(s.columns) for s in x_series]) >= 0.3
            ):
                res[i] = (new_x_series, new_y_series)
        return res

    def generate_col_vis_plan(
        self, x_series: List[Series], y_series: List[Series], merge_categories: bool = False
    ) -> List[ColVisPlan]:
        """Given x and y series, permute all possible aggregate functions f to
        generate visualization plans.

        Args:
            x_series: a list of x series
            y_series: a list of y series
        Returns:
            res: a list of ColVisPlan that share x and y, but different f
        """
        if not all(x.valid for x in x_series) and not all(y.valid for y in y_series):
            return []
        # simply return all possible aggregate functions, don't worry about optimization for now
        aggrs = list(Aggregate) if y_series[0].type == DataType.NUMERICAL else [Aggregate.COUNT]
        y_vis_plan: "list[ColVisPlan]" = [ColVisPlan(x_series, y_series, aggr, {}, merge_categories) for aggr in aggrs]
        return y_vis_plan

    @abstractmethod
    def compute_aggregate(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> Dict[Aggregate, Dict]:
        """Given x and y, compute result for all aggregate functions

        Args:
            x_series: a list of x series
            y_series: a list of y series
            aggrs: a list of aggregate functions
            buckets: same as the output of :func:`_compute_buckets`
        Returns:
            res: a list of dictionary that contains the results for each aggregate functions
        """
        raise NotImplementedError("Please Implement this method")

    @abstractmethod
    def find_top_k(self, k: int) -> List[ColVisPlan]:
        """Compute the top k visualization plans

        Args:
            k: top k
        Returns:
            res: a list of column visualization plan
        """
        raise NotImplementedError("Please Implement this method")
