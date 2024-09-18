#!/bin/bash

# open data
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

# tus
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 10 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 20 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 30 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 40 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 50 --threshold 0.2 \
                                    --table_search_result_path data/tus/tus_results \
                                    --query_path data/tus/query_new \
                                    --datalake_path data/tus/datalake_new \
                                    --orig_query_path data/tus/query \
                                    --orig_datalake_path data/tus/datalake \
                                    --result_path data/tus/basic_results -s

# santos as default
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 10 --threshold 0.2 --result_path data/santos/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 20 --threshold 0.2 --result_path data/santos/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 30 --threshold 0.2 --result_path data/santos/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 40 --threshold 0.2 --result_path data/santos/basic_results -s
pytest tests/test_basic_complete.py --vis_ins basic_merge --N 50 --threshold 0.2 --result_path data/santos/basic_results -s