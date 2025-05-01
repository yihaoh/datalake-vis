# Reproduction Instruction

This document give step-by-step procedures on reproducing the results in the expriments of the paper. Before getting started, please make sure you have installed the `poetry` tool mentioned in the [README.md](https://github.com/yihaoh/datalake-vis/blob/main/README.md#package-management). 


## Dataset Download
Please download the datasets following the links below:
1. [Santos](https://github.com/northeastern-datalab/santos): https://zenodo.org/records/7758091/files/santos_benchmark.zip?download=1
2. [TUS](https://github.com/RJMillerLab/table-union-search-benchmark): https://storage.googleapis.com/table-union-benchmark/large/benchmark.sqlite
3. [LakeBench](https://github.com/RLGen/LakeBench): https://drive.google.com/file/d/1ksOyaGVugeu7UJ0SKbYj4ri-rwgGfNH8/view?usp=drive_link

After downloading all data from the above sources, for each dataset, create separate folders to host them (named them `santos`, `tus`, `open_data` respectively). Under each folder, create folders named `query` and `datalake`, containing the query tables and data lake tables respectively. If a dataset does not specifically contains query tables, feel free to sample a set of query table from the data lake, and this should produce similar results.

Under the project home, create a new folder named `data`, and place `santos`, `tus` and `open_data` folders under the `data` folder. 

The data folder should look like the following after performing the above steps:

```
data
+--- santos
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
+--- tus
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
+--- open_data
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
```

## Table Union Search and Table Matching Information
We use [Starmie](https://github.com/megagonlabs/starmie) as the table union search engine. 

Run the Starmie engine with all the datasets with the following commands (we use Santos as an example, other datasets follow the same manner).

Pre-train:

```
CUDA_VISIBLE_DEVICES=0 python run_pretrain.py \
  --task santos \
  --batch_size 64 \
  --lr 5e-5 \
  --lm roberta \
  --n_epochs 3 \
  --max_len 128 \
  --size 10000 \
  --projector 768 \
  --save_model \
  --augment_op drop_col \
  --fp16 \
  --sample_meth tfidf_entity \
  --table_order column \
  --run_id 0
```

Model Inference:
```
python extractVectors.py \
  --benchmark santos \
  --table_order column \
  --run_id 0
```

For each query table under the `query` folder, search for result tables in `datalake`:
```
python test_naive_search.py \
  --encoder cl \
  --benchmark santos \
  --augment_op drop_col \
  --sample_meth tfidf_entity \
  --matching linear \
  --table_order column \
  --run_id 0 \
  --K 50 \
  --threshold 0.2
```

Modify the `test_naive_search.py` to have it output the table union search result. Store the result in a `.pkl` file using Python `Pickle` library. The result should be a dictionary that looks like the following:

```
{
    "query_table1.csv": {
        "result_table1.csv": [(0,1), (1,3), (2,2),...],
        "result_table2.csv": [(0,0), (1,1), (2,2),...],
        ...
    },
   "query_table2.csv": {
        "result_table1.csv": [(0,0), (1,1), (2,2),...],
        "result_table2.csv": [(0,0), (1,1), (2,2),...],
        ...
    },
}
```

To interpret the above, `query_table1.csv` has multiple result tables. For `result_table1.csv`, its column 1 is mapped to column 0 in `query_table1.csv`.

If you already have the table union search result in the aforementioned format, feel free not to run the Starmie engine.

Name the table union search result `results_k50_t2.pkl` and create a new folder named `tus_results` under the `santos` directory. 

Repeat the above for `tus` and `open_data`. After that, the `data` folder looks like the following:

```
data
+--- santos
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
     +--- tus_results
          |    results_k50_t2.pkl
+--- tus
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
     +--- tus_results
          |    results_k50_t2.pkl
+--- open_data
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
     +--- tus_results
          |    results_k50_t2.pkl
```

## Experiments for Execution Time and Effectiveness
Run the following commands under project home directory to reproduce the execution time and effectiveness results.

```
bash test_bash_command/test_basic_ind.sh
bash test_bash_command/test_basic_merge.sh
bash test_bash_command/test_opt_stats.sh
bash test_bash_command/test_opt_perf.sh
```

Note that there might be some randomness in the algorithm, so the results might slightly differ from the paper, but the general trend should be the same. The above commands might take quite some time to run, so it is recommended to do it through `tmux`.

After running the above scripts, the `data` folder under the project home should look like:

```
data
+--- santos
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
     +--- tus_results
          |    results_k50_t2.pkl
     +--- basic_results
          +--- results_N10_t2_basic_ind_top10
               |    run.log
          +--- results_N10_t2_basic_merge_top10
               |    run.log
          +--- results_N20_t2_basic_ind_top10
               |    run.log
          +--- results_N20_t2_basic_merge_top10
               |    run.log
          +--- results_N30_t2_basic_ind_top10
               |    run.log
          +--- results_N30_t2_basic_merge_top10
               |    run.log
          +--- results_N40_t2_basic_ind_top10
               |    run.log
          +--- results_N40_t2_basic_merge_top10
               |    run.log
          +--- results_N50_t2_basic_ind_top10
               |    run.log
          +--- results_N50_t2_basic_merge_top10
               |    run.log
    +--- opt_results
          +--- results_N10_t2_opt_stats_top10
               |    run.log
          +--- results_N10_t2_opt_perf_top10
               |    run.log
          +--- results_N20_t2_opt_stats_top10
               |    run.log
          +--- results_N20_t2_opt_perf_top10
               |    run.log
          +--- results_N30_t2_opt_stats_top10
               |    run.log
          +--- results_N30_t2_opt_perf_top10
               |    run.log
          +--- results_N40_t2_opt_stats_top10
               |    run.log
          +--- results_N40_t2_opt_perf_top10
               |    run.log
          +--- results_N50_t2_opt_stats_top10
               |    run.log
          +--- results_N50_t2_opt_perf_top10
               |    run.log
+--- tus
     ...
+--- open_data
     ...
```

Here `N10` means that we only take the top 10 relevant result tables from the table union search result. Each `run.log` contains the effectiveness and execution time summaries toward the end of the file.


## Experiments for Scalability
Based on the `open_data` folder, scale each csv file down to 20%, 40%, 60%, 80% and 100% to create 5 more folders `open_data_20`, `open_data_40`, `open_data_60`, `open_data_80`, `open_data_100`, which all have the same query and datalake as `open_data`. 

Run the following commands under project home directory for the scalability test:
```
bash test_bash_command/test_scalability_20.sh
bash test_bash_command/test_scalability_40.sh
bash test_bash_command/test_scalability_60.sh
bash test_bash_command/test_scalability_80.sh
bash test_bash_command/test_scalability_100.sh
```

Then you should see a similar folder structure as the previous step, and `run.log` contains the execution time and effectiveness summaries. 

Now run the following commands to reproduce scalability test result for different top-N result tables:
```
bash test_bash_command/test_scalability_100_top_5.sh
bash test_bash_command/test_scalability_100_top_10.sh
bash test_bash_command/test_scalability_100_top_20.sh
bash test_bash_command/test_scalability_100_top_50.sh
bash test_bash_command/test_scalability_100_top_100.sh
```

All results can be found under the `data/open_data_100` folder.

