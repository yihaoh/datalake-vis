#!/bin/bash

# open data
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

# tus
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 10 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 20 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 30 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 40 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 50 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/opt_results -s

# santos as default
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 10 --threshold 0.2 --result_path data/santos/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 20 --threshold 0.2 --result_path data/santos/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 30 --threshold 0.2 --result_path data/santos/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 40 --threshold 0.2 --result_path data/santos/opt_results -s
pytest tests/test_opt_stats_complete.py --vis_ins opt_stats --N 50 --threshold 0.2 --result_path data/santos/opt_results -s