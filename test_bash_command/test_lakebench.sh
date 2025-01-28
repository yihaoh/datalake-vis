#!/bin/bash

# basic no merge
pytest tests/test_basic_complete.py --vis_ins basic_ind --N 10 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_ind --N 20 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_ind --N 30 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_ind --N 40 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_ind --N 50 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s

# basic merge
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 10 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 20 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 30 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 40 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 50 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/basic_results -s

# stats
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 10 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 20 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 30 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 40 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 50 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s

# opt perf
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 10 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 20 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 30 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 40 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 50 --threshold 0.2 \
                                    --table_search_result_path data/open_data/tus_results \
                                    --query_path data/open_data/query_new \
                                    --datalake_path data/open_data/datalake_new \
                                    --orig_query_path data/open_data/query \
                                    --orig_datalake_path data/open_data/datalake \
                                    --result_path data/open_data/opt_results -s
                                    