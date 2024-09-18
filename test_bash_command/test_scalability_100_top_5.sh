#!/bin/bash


# opt_perf
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 10 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results \
                                    --K 5 -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 20 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results \
                                    --K 5 -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 30 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results \
                                    --K 5 -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 40 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results \
                                    --K 5 -s
pytest tests/test_opt_perf_complete.py --vis_ins opt_perf --N 50 --threshold 0.2 \
                                    --table_search_result_path data/open_data_100/tus_results \
                                    --query_path data/open_data_100/query_new \
                                    --datalake_path data/open_data_100/datalake_new \
                                    --orig_query_path data/open_data_100/query \
                                    --orig_datalake_path data/open_data_100/datalake \
                                    --result_path data/open_data_100/opt_results \
                                    --K 5 -s