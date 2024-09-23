""" Statistical Instance governing the visualization process """

import heapq
from abc import abstractmethod
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
from datalake_vis.utils import confidence_interval, find_all_pairs
from datalake_vis.vis_instance.interface import VisInstance

# pylint: disable=W0718


class OptVisInstance(VisInstance):
    """Statistics-based Visualization Instance"""

    def __init__(self, **kwarg):
        super().__init__(**kwarg)
        self.cached_count_data = {}
        self.cached_count_util = {}

    def compute_x_series(self, x: int) -> Tuple[List[Series], List, Any]:
        if self.query_table[x].data.isnull().all():
            return [], None, None
        c_type = self.query_table[x].type
        c_name = self.query_table[x].column_name
        # leave out the query table column for now
        candidate_columns: "list[Column]" = [
            t[self.column_matchings[t.name][x]]
            for t in self.result_tables
            if x in self.column_matchings[t.name]
            and t[self.column_matchings[t.name][x]].type == c_type
            and self.query_table[x].is_date == t[self.column_matchings[t.name][x]].is_date
        ]
        # use union find to determine merging
        u = UnionFind(elements=candidate_columns)  #  + [self.query_table[x]])
        candidate_series = [Series([self.query_table[x]], c_name)]
        for c1, c2 in find_all_pairs(candidate_columns):
            if c1.is_mergeable_with(c2):
                u.union(c1, c2)

        unions = list(u.to_sets())
        # if len(unions) > 2:  # we are guaranteed with 2 series after merging query table column
        #     for c in candidate_columns:
        #         if self.query_table[x].is_mergeable_with(c):
        #             u.union(self.query_table[x], c)
        #             break
        for union in unions:
            candidate_series.append(Series(list(union), c_name))

        x_series = candidate_series  # [Series([self.query_table[x]], c_name)] + candidate_series
        buckets, bucket_locator = self._compute_buckets(x, x_series)
        return x_series, buckets, bucket_locator

    @abstractmethod
    def optimize_computation(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> Dict[Aggregate, Dict]:
        """Apply optimization techniques for the computation of aggregate functions.

        Args:
            x_series: a list of x series
            y_series: a list of y series
            aggrs: a list of Enum aggregate functions that need to be computed
            buckets: categories to be grouped by into
            bucket_locator: a function/object that help locate the bucket for each cell value
        Return:
            aggregate_data: map aggregate functions to its result: category -> list of values,
            each of which is the value of the category from a series
        """
        raise NotImplementedError("Not implemented for this parent class.")

    def compute_aggregate(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> List[Dict]:
        raise NotImplementedError("Not implemented for this parent class")

    def find_top_k(self, k: int) -> List[ColVisPlan]:
        raise NotImplementedError("Not implemented for this parent class.")


# ======================================================================================
#                 Optimized VisInstance with stats-based merging series
# ======================================================================================


class OptStatsVisInstance(OptVisInstance):
    """All optimization without early stop"""

    def optimize_computation(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> Dict[Aggregate, Dict]:
        if not aggrs:
            return {}
        x_sig = (tuple(sorted([col.table_id for xs in x_series for col in xs.columns])), x_series[0].name)
        new_aggrs: List[Aggregate] = []
        plot_datas = {}
        if y_series[0].type == DataType.NUMERICAL:
            new_aggrs = (
                [aggr for aggr in aggrs if aggr != Aggregate.AVG]
                if x_sig not in self.cached_count_data
                else [aggr for aggr in aggrs if aggr != Aggregate.COUNT]
            )  # decide which aggregate to calculate
            plot_datas = self.compute_aggregate(x_series, y_series, new_aggrs, buckets, bucket_locator)

            # use or cache count result
            if x_sig in self.cached_count_data and Aggregate.COUNT in aggrs:
                plot_datas[Aggregate.COUNT] = self.cached_count_data[x_sig]
            elif x_sig not in self.cached_count_data and Aggregate.COUNT in plot_datas:
                self.cached_count_data[x_sig] = plot_datas[Aggregate.COUNT]

            # now make up average
            plot_datas[Aggregate.AVG] = {
                key: [
                    b / a if a != 0 else 0
                    for a, b in zip(plot_datas[Aggregate.COUNT][key], plot_datas[Aggregate.SUM][key])
                ]
                for key in plot_datas[Aggregate.COUNT].keys()
            }
        else:
            new_aggrs = [Aggregate.COUNT]
            if x_sig not in self.cached_count_data:
                plot_datas = self.compute_aggregate(x_series, y_series, new_aggrs, buckets, bucket_locator)
                self.cached_count_data[x_sig] = plot_datas[Aggregate.COUNT]
            else:
                plot_datas[Aggregate.COUNT] = self.cached_count_data[x_sig]
        return plot_datas

    def compute_aggregate(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> Dict[Aggregate, Dict]:
        res = {aggr: {b: [] for b in buckets} for aggr in aggrs}
        all_keys = set(buckets)
        data_dfs: List[pd.DataFrame] = []
        for xs, ys in zip(x_series, y_series):
            tp = pd.concat([xs.combine_data(), ys.combine_data()], axis=1)
            tp.columns = [0, 1]
            data_dfs.append(tp)

        for df in data_dfs:
            # for this series, compute aggregate result
            explored_keys = set()
            tp_df: pd.DataFrame = (
                df.groupby(df[0].apply(bucket_locator))[1].agg(
                    [aggr.value.name if aggr.value.built_in else aggr.value.aggregate for aggr in aggrs]
                )  # categorical data
                if x_series[0].type == DataType.CATEGORICAL
                else (
                    df.groupby(df[0].apply(lambda x: buckets[bucket_locator(buckets, x)]))[1].agg(
                        [aggr.value.name if aggr.value.built_in else aggr.value.aggregate for aggr in aggrs]
                    )
                    if x_series[0].type == DataType.NUMERICAL
                    or (
                        x_series[0].type == DataType.TEXTUAL and self.text_format == "numerical"
                    )  # numerical or textual as numerical
                    else df.groupby(df[0].apply(lambda x: bucket_locator.predict(np.array(x).reshape(1, -1))[0]))[
                        1
                    ].agg([aggr.value.name if aggr.value.built_in else aggr.value.aggregate for aggr in aggrs])
                )  # textual as vector
            )

            tp_df.columns = [aggr.value.name for aggr in aggrs]  # if aggr != Aggregate.AVG]
            # print(tp_df)
            # print(aggrs[0].value.name)
            for index, row in tp_df.iterrows():
                x = str(index) if x_series[0].type == DataType.CATEGORICAL else index
                explored_keys.add(x)
                for aggr in aggrs:
                    try:
                        res[aggr][x].append(row[aggr.value.name])
                    except Exception:
                        if isinstance(index, float):
                            res[aggr][str(int(index))].append(row[aggr.value.name])
                        elif isinstance(index, int):
                            res[aggr][str(float(index))].append(row[aggr.value.name])

            for key in all_keys.difference(explored_keys):
                for aggr in aggrs:
                    res[aggr][key].append(0)
        return res

    def find_top_k(self, k: int) -> List[ColVisPlan]:
        top_k_res = []
        for x in range(self.query_table.size()[1]):
            # merge series
            x_series, buckets, bucket_locator = self.compute_x_series(x)
            y_to_series = self.compute_y_series(x, x_series)

            # compute aggregate results, with shared computations
            # note that x series might not be the same across all pairs (as a y might be missing from a result table)
            # so we need to keep track of different count result
            for _, (x_series, y_series) in enumerate(y_to_series.values()):
                x_sig = tuple(sorted([col.table_id for xs in x_series for col in xs.columns]))
                y_vis_plans = []
                y_vis_plans = self.generate_col_vis_plan(x_series, y_series)

                tp_res = self.optimize_computation(
                    x_series, y_series, [plan.aggr for plan in y_vis_plans], buckets, bucket_locator
                )

                # compute utility, also share computations
                for plan in y_vis_plans:
                    plan.plot_data = tp_res[plan.aggr]
                    if plan.aggr != Aggregate.COUNT:
                        plan.compute_utility(EMD.compute_score)
                    elif plan.aggr == Aggregate.COUNT and x_sig not in self.cached_count_util:
                        plan.compute_utility(EMD.compute_score)
                        self.cached_count_util[x_sig] = plan.util_score
                    else:
                        plan.util_score = self.cached_count_util[x_sig]  #   can omit this line
                        continue  # no need to add this plan to heap since a count plan has been added

                    # now compute top k
                    if len(top_k_res) < k:
                        heapq.heappush(top_k_res, plan)
                    elif top_k_res[0] < plan:
                        heapq.heappop(top_k_res)
                        heapq.heappush(top_k_res, plan)
            # done for the column, clear cache
            self.cached_count_data.clear()
            self.cached_count_util.clear()
        return top_k_res


# ======================================================================================
#           Optimized VisInstance with stats-based merging series and pruning
# ======================================================================================


class OptPerfVisInstance(OptVisInstance):
    """Optimized basic instance with early stop batch processing"""

    def __init__(self, **kwarg):
        super().__init__(**kwarg)
        self.total_batch = kwarg["total_batch"] if "total_batch" in kwarg else 10  # default to prevent KeyError
        self.batch_idx = 0

    def optimize_computation(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> Dict[Aggregate, Dict]:
        if not x_series or not aggrs:
            return {}

        x_sig = (tuple(sorted([col.table_id for xs in x_series for col in xs.columns])), x_series[0].name)
        new_aggrs: List[Aggregate] = []
        plot_datas = {}
        if y_series[0].type == DataType.NUMERICAL:
            new_aggrs = (
                # [aggr for aggr in aggrs if aggr != Aggregate.AVG]
                aggrs
                if x_sig not in self.cached_count_data
                else [aggr for aggr in aggrs if aggr != Aggregate.COUNT]
            )  # decide which aggregate to calculate
            plot_datas = self.compute_aggregate(x_series, y_series, new_aggrs, buckets, bucket_locator)

            # use or cache count result
            if x_sig in self.cached_count_data and Aggregate.COUNT in aggrs:
                plot_datas[Aggregate.COUNT] = self.cached_count_data[x_sig]
            elif x_sig not in self.cached_count_data and Aggregate.COUNT in plot_datas:
                self.cached_count_data[x_sig] = plot_datas[Aggregate.COUNT]
        else:
            new_aggrs = [Aggregate.COUNT]
            if x_sig not in self.cached_count_data:
                plot_datas = self.compute_aggregate(x_series, y_series, new_aggrs, buckets, bucket_locator)
                self.cached_count_data[x_sig] = plot_datas[Aggregate.COUNT]
            else:
                plot_datas[Aggregate.COUNT] = self.cached_count_data[x_sig]
        return plot_datas

    def compute_aggregate(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggrs: List[Aggregate],
        buckets: List,
        bucket_locator: Any = None,
    ) -> Dict[Aggregate, Dict]:
        res = {aggr: {b: [] for b in buckets} for aggr in aggrs}
        all_keys = set(buckets)
        data_dfs: List[pd.DataFrame] = []
        for xs, ys in zip(x_series, y_series):
            batch_sz = xs.data.shape[0] // self.total_batch + 1
            start, end = self.batch_idx * batch_sz, (self.batch_idx + 1) * batch_sz
            data_dfs.append(pd.concat([xs.data[start:end], ys.data[start:end]], keys=[0, 1], axis=1))

        for df in data_dfs:
            # for this series, compute aggregate result
            explored_keys = set()
            tp_df: pd.DataFrame = (
                df.groupby(df[0].apply(bucket_locator))[1].agg(
                    [aggr.value.name if aggr.value.built_in else aggr.value.aggregate for aggr in aggrs]
                )  # categorical data
                if x_series[0].type == DataType.CATEGORICAL
                else (
                    df.groupby(df[0].apply(lambda x: buckets[bucket_locator(buckets, x)]))[1].agg(
                        [aggr.value.name if aggr.value.built_in else aggr.value.aggregate for aggr in aggrs]
                    )
                    if x_series[0].type == DataType.NUMERICAL
                    or (
                        x_series[0].type == DataType.TEXTUAL and self.text_format == "numerical"
                    )  # numerical or textual as numerical
                    else df.groupby(df[0].apply(lambda x: bucket_locator.predict(np.array(x).reshape(1, -1))[0]))[
                        1
                    ].agg(
                        aggrs[0].value.name
                    )  # textual as vector
                )
            )

            tp_df.columns = [aggr.value.name for aggr in aggrs]
            for index, row in tp_df.iterrows():
                x = str(index) if x_series[0].type == DataType.CATEGORICAL else index
                explored_keys.add(x)
                for aggr in aggrs:
                    try:
                        res[aggr][x].append(row[aggr.value.name])
                    except Exception:  # most likely KeyError due to combining mixed type
                        # after pd.concat, int column will be cast to float if float column exist in the series
                        if isinstance(index, float):
                            res[aggr][str(int(index))].append(row[aggr.value.name])
                        elif isinstance(index, int):
                            res[aggr][str(float(index))].append(row[aggr.value.name])

            for key in all_keys.difference(explored_keys):
                for aggr in aggrs:
                    res[aggr][key].append(0)
        return res

    def combine_aggregate(self, res1: Dict, res2: Dict, aggr: Aggregate):
        """Combine the aggregate results of two batches"""
        # print(res1, res2)
        res = {}
        if aggr in [Aggregate.COUNT, Aggregate.SUM]:
            for k, v in res1.items():
                res[k] = []
                for i, val in enumerate(v):
                    res[k].append(val + res2[k][i])
        elif aggr == Aggregate.AVG:  # might not be precise for last batch, but oh well...
            for k, v in res1.items():
                res[k] = []
                for i, val in enumerate(v):
                    res[k].append((val + res2[k][i]) / 2)
        elif aggr == Aggregate.MAX:
            for k, v in res1.items():
                res[k] = []
                for i, val in enumerate(v):
                    res[k].append(max(val, res2[k][i]))
        elif aggr == Aggregate.MIN:
            for k, v in res1.items():
                res[k] = []
                for i, val in enumerate(v):
                    res[k].append(min(val, res2[k][i]))
        return res

    def find_top_k(self, k: int) -> List[ColVisPlan]:
        all_plans: Dict[Tuple[str, str], Dict[Aggregate, ColVisPlan]] = {}
        all_buckets: Dict[Tuple[str, str], List] = {}
        all_bucket_locators: Dict[Tuple[str, str], Any] = {}
        series_data = {}
        # for each x column
        for x in range(self.query_table.size()[1]):
            x_series, buckets, bucket_locator = self.compute_x_series(x)
            y_to_series = self.compute_y_series(x, x_series)
            if not x_series:
                continue

            # buckets
            all_buckets[x_series[0].name] = buckets
            all_bucket_locators[x_series[0].name] = bucket_locator

            # (x,y) -> aggr -> ColVisPlan
            for xs, ys in y_to_series.values():
                all_plans[(xs[0].name, ys[0].name)] = {}
                vis_plans = self.generate_col_vis_plan(xs, ys)
                for plan in vis_plans:
                    all_plans[(xs[0].name, ys[0].name)][plan.aggr] = plan
                    series_data[(xs[0].name, ys[0].name)] = (xs, ys)

        # print(f"Total plan cnt: {plan_cnt}")

        while self.batch_idx < self.total_batch:
            for series, plans in all_plans.items():
                plan_data = self.optimize_computation(
                    series_data[series][0],
                    series_data[series][1],
                    [p.aggr for p in plans.values()],
                    all_buckets[series[0]],
                    all_bucket_locators[series[0]],
                )  # batch results

                # compute average EMD for each batch result and update
                for aggr, data in plan_data.items():
                    plans[aggr].util_score = (plans[aggr].util_score * self.batch_idx + EMD.compute_score(data)) / (
                        self.batch_idx + 1
                    )
                    plans[aggr].plot_data = (
                        data if not plans[aggr].plot_data else self.combine_aggregate(plans[aggr].plot_data, data, aggr)
                    )

            # compute EMD bounds for all (tuple of bound + plan)
            tp_all_plans: "list[tuple[tuple[int], ColVisPlan]]" = []
            ci = confidence_interval(self.batch_idx + 1, self.total_batch, 0.05)
            for aggr_to_plan in all_plans.values():
                for plan in aggr_to_plan.values():
                    tp_all_plans.append(((plan.util_score + ci, plan.util_score - ci), plan))
            tp_all_plans.sort(reverse=True)

            # no valid plan generated, just move on
            if not tp_all_plans:
                break

            # figure out top-k so far and discard some plans in all_plans
            # upper = max([pair[0][0] for pair in tp_all_plans[:k]])
            lower = min([pair[0][1] for pair in tp_all_plans[:k]])
            for i in range(k, len(tp_all_plans)):
                if tp_all_plans[i][1].util_score == 0 or tp_all_plans[i][0][0] <= lower:
                    series = (tp_all_plans[i][1].x_name, tp_all_plans[i][1].y_name)
                    all_plans[series].pop(tp_all_plans[i][1].aggr)

            self.top_k = tp_all_plans[:k]

            # if only k left, done
            if sum(len(plans) for plans in all_plans) == k:
                break

            # increment batch
            self.batch_idx += 1

        self.cached_count_data.clear()
        for plans in all_plans.values():
            for plan in plans.values():
                # update top-k
                if len(self.top_k) < k:
                    heapq.heappush(self.top_k, plan)
                else:
                    heapq.heappushpop(self.top_k, plan)
        return self.top_k
