#!/bin/bash


# opt_perf
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 10 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 20 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 30 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 40 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 50 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results -s


# # basic_ind
# pytest tests/test_basic_ind_complete.py --vis_ins basic_ind --N 10 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_ind_complete.py --vis_ins basic_ind --N 20 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_ind_complete.py --vis_ins basic_ind --N 30 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_ind_complete.py --vis_ins basic_ind --N 40 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_ind_complete.py --vis_ins basic_ind --N 50 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s


# # basic_merge
# pytest tests/test_basic_merge_complete.py --vis_ins basic_merge --N 10 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_merge_complete.py --vis_ins basic_merge --N 20 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_merge_complete.py --vis_ins basic_merge --N 30 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_merge_complete.py --vis_ins basic_merge --N 40 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_basic_merge_complete.py --vis_ins basic_merge --N 50 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s


# # scale: opt_stats
# pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 10 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 20 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 30 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 40 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s
# pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 50 --threshold 0.2 \
#                                     --table_search_result_path data/open_data_100/tus_results \
#                                     --query_path data/open_data_100/query_new \
#                                     --datalake_path data/open_data_100/datalake_new \
#                                     --orig_query_path data/open_data_100/query \
#                                     --orig_datalake_path data/open_data_100/datalake \
#                                     --result_path data/open_data_100/opt_results -s

