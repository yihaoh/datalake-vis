""" Complete Optimization Test """

import heapq
import math
import os
import pickle
import time
import traceback
from typing import Any

import pytest

from datalake_vis.aggr import Aggregate
from datalake_vis.col_vis_plan import ColVisPlan
from datalake_vis.util_functions import EMD
from datalake_vis.utils import confidence_interval, plot_vis_plan
from datalake_vis.vis_instance.opt_vis_instance import OptPerfVisInstance


# pylint: disable=W0718, W1514, C0116, W0621
@pytest.fixture
def vis_args(request) -> "dict[str, str]":
    return {
        "table_search_result_path": request.config.getoption("--table_search_result_path"),
        "query_path": request.config.getoption("--query_path"),
        "orig_query_path": request.config.getoption("--orig_query_path"),
        "datalake_path": request.config.getoption("--datalake_path"),
        "orig_datalake_path": request.config.getoption("--orig_datalake_path"),
        "result_path": request.config.getoption("--result_path"),
        "threshold": int(request.config.getoption("--threshold").split(".")[1]),
        "N": int(request.config.getoption("--N")),
        "vis_ins": request.config.getoption("--vis_ins"),
        "K": request.config.getoption("--K"),
        "bucket_num": request.config.getoption("--bucket_num"),
        "preprocessed_data": request.config.getoption("--preprocessed_data"),
        "total_batch": request.config.getoption("--total_batch"),
        "text_format": request.config.getoption("--text_format"),
    }


def test_opt_perf_complete(vis_args):

    tus_res_file = open(
        # f"{vis_args['table_search_result_path']}/results_k{vis_args['K']}_t{vis_args['threshold']}.pkl",
        f"{vis_args['table_search_result_path']}/results_k50_t{vis_args['threshold']}.pkl",
        "rb",
    )
    tus_res: dict = pickle.load(tus_res_file)
    tus_res_file.close()

    # RUN_RES_PATH = f"{vis_args['result_path']}/results_k{vis_args['K']}_t{vis_args['threshold']}_{vis_args['vis_ins']}"
    RUN_RES_PATH = f"{vis_args['result_path']}/results_N{vis_args['N']}_t{vis_args['threshold']}_{vis_args['vis_ins']}_top_{vis_args['K']}"
    os.makedirs(RUN_RES_PATH, exist_ok=True)

    vis_res_log = open(f"{RUN_RES_PATH}/run.log", "w")
    vis_err_log = open(f"{RUN_RES_PATH}/error.log", "w")

    query_times: "list[list[float]]" = []
    col_vis_plan_cnt: "list[int]" = []  # list of ColVisPlan per query
    col_vis_plan_util: "list[float]" = []  # list of total util per query
    col_vis_plan_util_upper: "list[float]" = []
    col_vis_plan_util_lower: "list[float]" = []

    # for every query table
    query_cnt = 0
    invalid_query_cnt = 0
    total_time = 0
    entire_test_start = time.time()
    for query, matchings in tus_res.items():
        if query_cnt > 0 and query_cnt % 5 == 0:
            print(f"Complete {query_cnt}/{len(tus_res)} queries")
        # if query_cnt >= 1:
        #     break
        # if query != "lane_description_a.csv":
        #     continue
        query_cnt += 1

        args = {
            "query_path": f"{vis_args['query_path']}/{query}",
            "result_paths": [
                f"{vis_args['datalake_path']}/{v[0]}" for i, v in enumerate(matchings) if i < vis_args["N"]
            ],
            "query_table_name": query,
            "result_table_names": [v[0] for i, v in enumerate(matchings) if i < vis_args["N"]],
            "column_matchings": matchings,
            "orig_query_path": f"{vis_args['orig_query_path']}/{query}",
            "orig_result_paths": [
                f"{vis_args['orig_datalake_path']}/{v[0]}" for i, v in enumerate(matchings) if i < vis_args["N"]
            ],
            "bucket_num": int(vis_args["bucket_num"]),
            "preprocessed_data": vis_args["preprocessed_data"] == "True",
            "total_batch": int(vis_args["total_batch"]),
            "text_format": vis_args["text_format"],
        }

        # setup figure path
        # os.makedirs(f"{RUN_RES_PATH}/figures/{query.split('.')[0]}", exist_ok=True)
        K = int(vis_args["K"])
        try:
            vis_ins = OptPerfVisInstance(**args)
        except Exception as e:
            vis_err_log.write(f"=========================== {query} is INVALID ===========================\n")
            vis_err_log.write(str(e) + "\n" + traceback.format_exc())
            invalid_query_cnt += 1
            continue

        # collect stats
        plan_gen_time = 0
        plan_aggr_time = 0
        plan_util_time = 0
        plan_util = 0
        plan_util_lower = 0
        plan_util_upper = 0
        plan_cnt = 0
        vis_res_log.write(f"=========================== {vis_ins.query_table.name} ===========================\n")
        vis_err_log.write(f"=========================== {vis_ins.query_table.name} ===========================\n")

        all_plans: "dict[tuple[str,str], dict[Aggregate, ColVisPlan]]" = {}
        top_k_util_lower = math.inf
        all_buckets: "dict[tuple[str,str], list]" = {}
        all_bucket_locators: "dict[tuple[str,str], Any]" = {}
        series_data = {}
        # final_res = []
        total_time_sub = time.time()
        # for each x column
        for x in range(vis_ins.query_table.size()[1]):
            try:
                sub_start = time.time()
                x_series, buckets, bucket_locator = vis_ins.compute_x_series(x)
                if not x_series:
                    continue
                y_to_series = vis_ins.compute_y_series(x, x_series)
                plan_gen_time += time.time() - sub_start
            except Exception as e:
                vis_err_log.write(f"Error when computing series for column {x}, {vis_ins.query_table[x].column_name}\n")
                vis_err_log.write(str(e) + "\n" + traceback.format_exc())
                continue

            # buckets
            all_buckets[x_series[0].name] = buckets
            all_bucket_locators[x_series[0].name] = bucket_locator

            # (x,y) -> aggr -> ColVisPlan
            for xs, ys in y_to_series.values():
                all_plans[(xs[0].name, ys[0].name)] = {}
                vis_plans = vis_ins.generate_col_vis_plan(xs, ys)
                plan_cnt += len(vis_plans)
                for plan in vis_plans:
                    all_plans[(xs[0].name, ys[0].name)][plan.aggr] = plan
                    series_data[(xs[0].name, ys[0].name)] = (xs, ys)

        while vis_ins.batch_idx < vis_ins.total_batch:
            for series, plans in all_plans.items():
                try:
                    sub_start = time.time()
                    plan_data = vis_ins.optimize_computation(
                        series_data[series][0],
                        series_data[series][1],
                        [p.aggr for p in plans.values()],
                        all_buckets[series[0]],
                        all_bucket_locators[series[0]],
                    )  # batch results

                    # compute average EMD for each batch result and update
                    for aggr, data in plan_data.items():
                        plans[aggr].util_score = (
                            plans[aggr].util_score * vis_ins.batch_idx + EMD.compute_score(data)
                        ) / (vis_ins.batch_idx + 1)
                        plans[aggr].plot_data = (
                            data
                            if not plans[aggr].plot_data
                            else vis_ins.combine_aggregate(plans[aggr].plot_data, data, aggr)
                        )
                    plan_aggr_time += time.time() - sub_start
                except Exception as e:
                    vis_err_log.write(f"Error when computing GROUP-BY for (x,y): {series}\n")
                    vis_err_log.write(str(e) + "\n" + traceback.format_exc())
                    break

            try:
                sub_start = time.time()
                # compute EMD bounds for all (tuple of bound + plan)
                tp_all_plans: "list[tuple[tuple[int], ColVisPlan]]" = []
                ci = confidence_interval(vis_ins.batch_idx + 1, vis_ins.total_batch, 0.05)
                for aggr_to_plan in all_plans.values():
                    for plan in aggr_to_plan.values():
                        tp_all_plans.append(((plan.util_score + ci, plan.util_score - ci), plan))
                tp_all_plans.sort(reverse=True)

                # no valid plan generated, just move on
                if not tp_all_plans:
                    break

                # figure out top-k so far and discard some plans in all_plans
                # upper = max([pair[0][0] for pair in tp_all_plans[:k]])
                lower = min([pair[0][1] for pair in tp_all_plans[:K]])
                for i in range(K, len(tp_all_plans)):
                    if tp_all_plans[i][1].util_score == 0 or tp_all_plans[i][0][0] <= lower:
                        series = (tp_all_plans[i][1].x_name, tp_all_plans[i][1].y_name)

                        # record discarded plan stats
                        # vis_res_log.write("prune the following:\n")
                        # vis_res_log.write(str(all_plans[series][tp_all_plans[i][1].aggr]))
                        plan_util += all_plans[series][tp_all_plans[i][1].aggr].util_score
                        plan_util_lower += tp_all_plans[i][0][1]
                        plan_util_upper += tp_all_plans[i][0][0]

                        all_plans[series].pop(tp_all_plans[i][1].aggr)
                        # if not all_plans[series]:
                        #     all_plans.pop(series)
                plan_util_time += time.time() - sub_start
            except Exception as e:
                vis_err_log.write("Error when computing confidence interval and discard plan.\n")
                vis_err_log.write(str(e) + "\n" + traceback.format_exc())
                break

            # if only k left, done
            if sum([len(plans) for plans in all_plans]) <= K:
                break

            # increment batch
            vis_ins.batch_idx += 1
            # vis_res_log.write("=========== Done Pruning ============\n")

        vis_ins.cached_count_data.clear()

        total_time += time.time() - total_time_sub
        for plans in all_plans.values():
            for plan in plans.values():
                # update top-k
                if len(vis_ins.top_k) < K:
                    heapq.heappush(vis_ins.top_k, plan)
                else:
                    heapq.heappushpop(vis_ins.top_k, plan)

                # record
                # vis_res_log.write(str(plan))
                plan_util += plan.util_score
                plan_util_lower += plan.util_score
                plan_util_upper += plan.util_score

        # collect result
        query_times.append([plan_gen_time, plan_aggr_time, plan_util_time])
        col_vis_plan_util.append(plan_util)
        col_vis_plan_util_lower.append(plan_util_lower)
        col_vis_plan_util_upper.append(plan_util_upper)
        col_vis_plan_cnt.append(plan_cnt)

    # write summary to log
    vis_res_log.write("=========================== Summary ===========================\n")
    vis_res_log.write(f"Total time including all processing: {time.time() - entire_test_start}\n")
    vis_res_log.write(f"Total query: {query_cnt}, valid query: {query_cnt - invalid_query_cnt}\n")
    vis_res_log.write(f"Total time for {query_cnt - invalid_query_cnt} queries: {total_time}\n")
    vis_res_log.write(f"Avg time for each query: {total_time / (query_cnt - invalid_query_cnt)}\n")
    vis_res_log.write(
        f"Avg time for ColVisPlan within each query (generation to util computation): \
            {[sum(sub) / col_vis_plan_cnt[i] if col_vis_plan_cnt[i] != 0 else 0 for i, sub in enumerate(query_times)]}\n"
    )
    vis_res_log.write(f"Avg time for ColVisPlan: {sum([sum(sub) for sub in query_times]) / sum(col_vis_plan_cnt)}\n")
    vis_res_log.write(
        f"Avg time for ColVisPlan compute series: {sum([sub[0] for sub in query_times]) / sum(col_vis_plan_cnt)}\n"
    )
    vis_res_log.write(
        f"Avg time for ColVisPlan compute group-by results: {sum([sub[1] for sub in query_times]) / sum(col_vis_plan_cnt)}\n"
    )
    vis_res_log.write(
        f"Avg time for ColVisPlan compute utility: {sum([sub[2] for sub in query_times]) / sum(col_vis_plan_cnt)}\n"
    )
    vis_res_log.write(f"Avg utility for all ColVisPlan: {sum(col_vis_plan_util) / sum(col_vis_plan_cnt)}\n")
    vis_res_log.write(f"Avg utility for all ColVisPlan upper: {sum(col_vis_plan_util_upper) / sum(col_vis_plan_cnt)}\n")
    vis_res_log.write(f"Avg utility for all ColVisPlan lower: {sum(col_vis_plan_util_lower) / sum(col_vis_plan_cnt)}\n")

    vis_res_log.write("=========================== Raw Statistics ===========================\n")
    vis_res_log.write(f"Query time breakdown for each query: {str(query_times)}\n")
    vis_res_log.write(f"Utility average of ColVisPlan per query: {str(col_vis_plan_util)}\n")
    vis_res_log.write(f"Number of ColVisPlan per query: {str(col_vis_plan_cnt)}\n")

    vis_res_log.close()
    vis_err_log.close()
