""" Complete Optimization Test """

import heapq
import os
import pickle
import time
import traceback

import pytest

from datalake_vis.aggr import Aggregate
from datalake_vis.util_functions import EMD
from datalake_vis.utils import plot_vis_plan
from datalake_vis.vis_instance.opt_vis_instance import OptStatsVisInstance


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
        "text_format": request.config.getoption("--text_format"),
        "target_query": request.config.getoption("--target_query"),
    }


def test_opt_complete(vis_args):

    tus_res_file = open(
        # f"{vis_args['table_search_result_path']}/results_k{vis_args['K']}_t{vis_args['threshold']}.pkl",
        f"{vis_args['table_search_result_path']}/results_k50_t{vis_args['threshold']}.pkl",
        "rb",
    )
    tus_res: dict = pickle.load(tus_res_file)
    tus_res_file.close()

    # RUN_RES_PATH = f"{vis_args['result_path']}/results_k{vis_args['K']}_t{vis_args['threshold']}_{vis_args['vis_ins']}"
    RUN_RES_PATH = f"{vis_args['result_path']}/results_k{vis_args['N']}_t{vis_args['threshold']}_{vis_args['vis_ins']}"
    os.makedirs(RUN_RES_PATH, exist_ok=True)

    vis_res_log = open(f"{RUN_RES_PATH}/run.log", "w")
    vis_err_log = open(f"{RUN_RES_PATH}/error.log", "w")

    query_times: "list[list[float]]" = []
    col_vis_plan_cnt: "list[int]" = []  # list of ColVisPlan per query
    col_vis_plan_util: "list[float]" = []  # list of total util per query

    # for every query table
    query_cnt = 0
    invalid_query_cnt = 0
    total_time = 0
    entire_test_start = time.time()
    for query, matchings in tus_res.items():
        if vis_args["target_query"] != "all" and query != vis_args["target_query"]:
            continue
        if query_cnt > 0 and query_cnt % 5 == 0:
            print(f"Complete {query_cnt}/{len(tus_res)} queries")
        # if query_cnt >= 1:
        #     break
        query_cnt += 1

        args = {
            "query_path": f"{vis_args['query_path']}/{query}",
            "result_paths": [
                f"{vis_args['datalake_path']}/{v[0]}"
                for i, v in enumerate(matchings)
                if i < vis_args["N"] and v[0] != query
            ],
            "query_table_name": query,
            "result_table_names": [v[0] for i, v in enumerate(matchings) if i < vis_args["N"] and v[0] != query],
            "column_matchings": matchings,
            "orig_query_path": f"{vis_args['orig_query_path']}/{query}",
            "orig_result_paths": [
                f"{vis_args['orig_datalake_path']}/{v[0]}"
                for i, v in enumerate(matchings)
                if i < vis_args["N"] and v[0] != query
            ],
            "bucket_num": int(vis_args["bucket_num"]),
            "preprocessed_data": vis_args["preprocessed_data"] == "True",
            "text_format": vis_args["text_format"],
        }

        # setup figure path
        # os.makedirs(f"{RUN_RES_PATH}/figures/{query.split('.')[0]}", exist_ok=True)

        try:
            vis_ins = OptStatsVisInstance(**args)
        except Exception as e:
            vis_err_log.write(f"=========================== {query} is INVALID ===========================\n")
            vis_err_log.write(str(e) + "\n" + traceback.format_exc())
            invalid_query_cnt += 1
            continue
        plan_gen_time = 0
        plan_aggr_time = 0
        plan_util_time = 0
        plan_util = 0
        plan_cnt = 0
        K = int(vis_args["K"])
        vis_res_log.write(f"=========================== {vis_ins.query_table.name} ===========================\n")
        vis_err_log.write(f"=========================== {vis_ins.query_table.name} ===========================\n")

        total_time_sub = time.time()
        # for each x column
        for x in range(vis_ins.query_table.size()[1]):
            try:
                sub_start = time.time()
                x_series, buckets, bucket_locator = vis_ins.compute_x_series(x)
                y_to_series = vis_ins.compute_y_series(x, x_series)
                # print(y_to_series)
                plan_gen_time += time.time() - sub_start
            except Exception as e:
                vis_err_log.write(f"Error when computing series for column {x}, {vis_ins.query_table[x].column_name}\n")
                vis_err_log.write(str(e) + "\n" + traceback.format_exc())
                continue

            # for each y column
            for _, (x_series, y_series) in enumerate(y_to_series.values()):
                y_vis_plans = []
                x_sig = tuple(sorted([col.table_id for xs in x_series for col in xs.columns]))
                try:
                    sub_start = time.time()
                    y_vis_plans = vis_ins.generate_col_vis_plan(x_series, y_series)
                    tp_res = vis_ins.optimize_computation(
                        x_series, y_series, [plan.aggr for plan in y_vis_plans], buckets, bucket_locator
                    )
                    plan_aggr_time += time.time() - sub_start
                except Exception as e:
                    vis_err_log.write(
                        f"Error when generating ColVisPlan for column {x}, {vis_ins.query_table[x].column_name}\n"
                    )
                    vis_err_log.write(str(e) + "\n" + traceback.format_exc())
                    continue

                # compute utility, also share computations
                for plan in y_vis_plans:
                    plan.plot_data = tp_res[plan.aggr]
                    omit_plan = False
                    try:
                        sub_start = time.time()
                        if plan.aggr != Aggregate.COUNT:
                            plan.compute_utility(EMD.compute_score)
                        elif plan.aggr == Aggregate.COUNT and x_sig not in vis_ins.cached_count_util:
                            plan.compute_utility(EMD.compute_score)
                            vis_ins.cached_count_util[x_sig] = plan.util_score
                        else:
                            plan.util_score = vis_ins.cached_count_util[x_sig]  #   can omit this line
                            omit_plan = True
                        plan_util_time += time.time() - sub_start
                        plan_cnt += 1
                        plan_util += plan.util_score
                    except Exception as e:
                        vis_err_log.write(f"Error when computing utility for {str(plan)}")
                        vis_err_log.write(str(e) + "\n" + traceback.format_exc())
                        vis_err_log.write(str(plan.plot_data) + "\n")
                        continue

                    # vis_res_log.write(str(plan))
                    # if len(plan.plot_data.keys()) < 50:  # visualize only if not many categories
                    #     plot_vis_plan(
                    #         plan.plot_data,
                    #         plan.x_name,
                    #         plan.y_name,
                    #         plan.aggr.value,
                    #         f"{RUN_RES_PATH}/figures/{vis_ins.query_table.name.split('.')[0]}",
                    #     )
                    if not omit_plan:
                        if len(vis_ins.top_k) < K:
                            heapq.heappush(vis_ins.top_k, plan)
                        else:
                            heapq.heappushpop(vis_ins.top_k, plan)
            vis_ins.cached_count_data.clear()
            vis_ins.cached_count_util.clear()

        # collect result
        total_time += time.time() - total_time_sub
        query_times.append([plan_gen_time, plan_aggr_time, plan_util_time])
        col_vis_plan_util.append(plan_util)
        col_vis_plan_cnt.append(plan_cnt)
        vis_ins.export_results_to_json(f"frontend/frontend-data/{query.split('.')[0]}.json")

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

    vis_res_log.write("=========================== Raw Statistics ===========================\n")
    vis_res_log.write(f"Query time breakdown for each query: {str(query_times)}\n")
    vis_res_log.write(f"Utility average of ColVisPlan per query: {str(col_vis_plan_util)}\n")
    vis_res_log.write(f"Number of ColVisPlan per query: {str(col_vis_plan_cnt)}\n")

    vis_res_log.close()
    vis_err_log.close()
