""" Visualization Instance governing the visualization process """

import heapq
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from networkx.utils.union_find import UnionFind

from datalake_vis.aggr import Aggregate
from datalake_vis.col_vis_plan import ColVisPlan
from datalake_vis.column import Column
from datalake_vis.data_types import DataType
from datalake_vis.series import Series
from datalake_vis.util_functions import EMD
from datalake_vis.vis_instance.interface import VisInstance


class BasicVisInstance(VisInstance):
    """Basic implementation of visualization instance"""

    def compute_x_series(self, x: int) -> Tuple[List[Series], List, None]:
        raise NotImplementedError("Not implemented for this parent class")

    def compute_aggregate(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> Dict[Aggregate, Dict]:
        data_dfs: List[pd.DataFrame] = []
        all_keys = set(buckets)
        res_plot_data = {b: [] for b in buckets}
        for xs, ys in zip(x_series, y_series):
            tp = pd.concat([xs.combine_data(), ys.combine_data()], axis=1)
            tp.columns = [0, 1]
            data_dfs.append(tp)

        if x_series[0].type == DataType.CATEGORICAL:
            for df in data_dfs:
                explored_keys = set()
                for x, y in df.groupby(df[0].apply(bucket_locator))[1].agg(aggrs[0].value.name).items():
                    explored_keys.add(str(x))
                    try:
                        res_plot_data[str(x)].append(y)
                    except Exception:  # most likely KeyError due to combining mixed type
                        # after pd.concat, int column will be cast to float if float column exist in the series
                        if isinstance(x, float):
                            res_plot_data[str(int(x))].append(y)
                        elif isinstance(x, int):
                            res_plot_data[str(float(x))].append(y)

                for key in all_keys.difference(explored_keys):
                    res_plot_data[key].append(0)
        elif x_series[0].type == DataType.NUMERICAL or (
            x_series[0].type == DataType.TEXTUAL and self.text_format == "numerical"
        ):  # NUMERICAL or TEXTUAL as numerical
            for df in data_dfs:
                explored_keys = set()
                for x, y in (
                    df.groupby(df[0].apply(lambda x: buckets[bucket_locator(buckets, x)]))[1]
                    .agg(aggrs[0].value.name)
                    .items()
                ):
                    res_plot_data[x].append(y)
                    explored_keys.add(x)
                for key in all_keys.difference(explored_keys):
                    res_plot_data[key].append(0)
        else:  # TEXTUAL as "vector", TODO: check
            for df in data_dfs:
                explored_keys = set()
                for x, y in (
                    df.groupby(df[0].apply(lambda x: bucket_locator.predict(np.array(x).reshape(1, -1))))[1]
                    .agg(aggrs[0].value.name)
                    .items()
                ):
                    res_plot_data[x].append(y)
                    explored_keys.add(x)
                for key in all_keys.difference(explored_keys):
                    res_plot_data[key].append(0)
        return {aggrs[0]: res_plot_data}

    def find_top_k(self, k: int) -> List[ColVisPlan]:
        for x in range(self.query_table.size()[1]):
            x_series, buckets, bucket_locator = self.compute_x_series(x)
            y_to_series = self.compute_y_series(x, x_series)
            for _, (x_series, y_series) in enumerate(y_to_series.values()):
                y_vis_plans = self.generate_col_vis_plan(x_series, y_series)  # generate all suitable f
                for _, plan in enumerate(y_vis_plans):
                    plan.plot_data = self.compute_aggregate(x_series, y_series, buckets, bucket_locator)
                    plan.compute_utility(EMD.compute_score)

                    # now compute top k
                    if len(self.top_k) < k:
                        heapq.heappush(self.top_k, plan)
                    elif self.top_k[0] < plan:
                        heapq.heappop(self.top_k)
                        heapq.heappush(self.top_k, plan)
        return self.top_k


class BasicIndVisInstance(BasicVisInstance):
    """Basic implementation with no merging series"""

    def compute_x_series(self, x: int) -> Tuple[List[Series], List, Any]:
        if self.query_table[x].data.isnull().all():
            return [], None, None
        c_name = self.query_table[x].column_name
        x_series = [Series([self.query_table[x]], c_name)] + [
            Series([t[self.column_matchings[t.name][x]]], c_name)
            for t in self.result_tables
            if x in self.column_matchings[t.name]
            and t[self.column_matchings[t.name][x]].type == self.query_table[x].type
            and self.query_table[x].is_date == t[self.column_matchings[t.name][x]].is_date
        ]
        buckets, bucket_locator = self._compute_buckets(x, x_series)
        return x_series, buckets, bucket_locator


class BasicMergeVisInstance(BasicVisInstance):
    """Basic implementation with merging series"""

    def compute_x_series(self, x: int) -> Tuple[List[Series], List, Any]:
        if self.query_table[x].data.isnull().all():
            return [], None, None
        # first discard columns that have a different type from the query table column
        c_type = self.query_table[x].type
        c_name = self.query_table[x].column_name
        candidate_columns: List[Column] = [
            t[self.column_matchings[t.name][x]]
            for t in self.result_tables
            if x in self.column_matchings[t.name]
            and t[self.column_matchings[t.name][x]].type == c_type
            and self.query_table[x].is_date == t[self.column_matchings[t.name][x]].is_date
        ]

        # heuristic grouping happens here
        u = UnionFind(elements=candidate_columns)
        candidate_series = []
        for i, c_i in enumerate(candidate_columns):
            for j, c_j in enumerate(candidate_columns):
                if i < j and c_i.check_overlap(c_j) > 0.5:
                    u.union(c_i, c_j)
        for union in u.to_sets():
            candidate_series.append(Series(list(union), c_name))

        # don't forget add column c
        x_series = [Series([self.query_table[x]], c_name)] + candidate_series
        buckets, bucket_locator = self._compute_buckets(x, x_series)
        return x_series, buckets, bucket_locator
