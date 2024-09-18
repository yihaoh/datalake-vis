""" Column Visulization Plan """

from datetime import datetime
from typing import Dict, List

from networkx.utils.union_find import UnionFind
from scipy.stats import chi2_contingency, kstest

from datalake_vis.aggr import Aggregate
from datalake_vis.data_types import DataType
from datalake_vis.series import Series
from datalake_vis.utils import find_all_pairs


class ColVisPlan:
    """Column Visualizaiton Plan"""

    def __init__(
        self,
        x_series: List[Series],
        y_series: List[Series],
        aggr: Aggregate,
        plot_data: Dict,
        merge_category: bool = False,
    ):
        # metadata
        self.x_series: List[Series] = x_series
        self.y_series: List[Series] = y_series
        self.x_type: DataType = self.x_series[0].type
        self.y_type: DataType = self.y_series[0].type
        self.x_name: str = self.x_series[0].name
        self.y_name: str = self.y_series[0].name
        self.aggr: Aggregate = aggr

        # computed data
        self.plot_data = (
            self._update_key_for_date(plot_data)
            if self.x_type == DataType.NUMERICAL and x_series[0].is_date
            else plot_data
        )  # category --> list[aggr(y)]
        self.util_score: float = 0.0

        if merge_category:
            self._merge_categories()

    def _update_key_for_date(self, data: Dict) -> Dict:
        return {(datetime.fromordinal(k[0]), datetime.fromordinal(k[1])): v for k, v in data.items()}

    def _merge_categories(self) -> None:
        all_cats = list(self.plot_data.keys())
        u = UnionFind(elements=all_cats)
        for c1, c2 in find_all_pairs(all_cats):
            dist = [[n1, n2] for n1, n2 in zip(self.plot_data[c1], self.plot_data[c2])]
            try:
                if (
                    chi2_contingency(dist).pvalue > 0.05
                    and kstest(self.plot_data[c1], self.plot_data[c2]).pvalue > 0.05
                ):
                    # in order for two categories to merge, they must be similar in shape and scale
                    u.union(c1, c2)
            except Exception:
                # if dist contains 0, cannot run test, then don't merge
                continue
        new_plot_data = {}
        for union in u.to_sets():
            new_key = tuple(union)
            # TODO: still ad-hoc treatment, later find ways to generalize
            if self.aggr in [Aggregate.COUNT, Aggregate.SUM]:
                new_plot_data[new_key] = [sum(t) for t in zip(*[self.plot_data[key] for key in new_key])]
            elif self.aggr == Aggregate.AVG:
                # note this is not the accurate way to compute average, but oh well, it's a good estimate
                new_plot_data[new_key] = [sum(t) / len(new_key) for t in zip(*[self.plot_data[key] for key in new_key])]
            elif self.aggr == Aggregate.MIN:
                new_plot_data[new_key] = [min(t) for t in zip(*[self.plot_data[key] for key in new_key])]
            elif self.aggr == Aggregate.MAX:
                new_plot_data[new_key] = [max(t) for t in zip(*[self.plot_data[key] for key in new_key])]
        self.plot_data = new_plot_data

    def __repr__(self):
        return f"x: {(self.x_name, self.x_type.value)} | y: {(self.y_name, self.y_type.value)} | f: {self.aggr.value.name} | util: {self.util_score} | series: {len(self.x_series)} | categories: {len(self.plot_data.keys())}\n"

    def __str__(self):
        return f"x: {(self.x_name, self.x_type.value)} | y: {(self.y_name, self.y_type.value)} | f: {self.aggr.value.name} | util: {self.util_score} | series: {len(self.x_series)} | categories: {len(self.plot_data.keys())}\n"

    def __lt__(self, other: "ColVisPlan"):
        return self.util_score < other.util_score

    def compute_utility(self, util_func: callable):
        """compute utility using util_func"""
        self.util_score = util_func(self.plot_data)
        return self.util_score

    def plan_details(self):
        """print the details of ColVisPlan"""
        header = f"==================== {self.x_name} vs. {self.y_name} ===================="
        summary = (
            f"x: {(self.x_name, self.x_type.value)} | y: {(self.y_name, self.y_type.value)} | f: {self.aggr.value.name}"
        )
        x_series = f"Count: {len(self.x_series)} | X: {'; '.join([str(xs) for xs in self.x_series])}"
        y_series = f"Count: {len(self.y_series)} | Y: {'; '.join([str(ys) for ys in self.y_series])}"
        score = f"util: {self.util_score}"
        return "\n".join([header, summary, x_series, y_series, score])
