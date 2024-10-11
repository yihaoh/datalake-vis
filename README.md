# datalake-vis

## Repository Structure
```
.
+--- README.md
+--- pyproject.toml
+--- .gitignore
+--- src/datalake_vis
     |    table.py
     |    column.py
     |    series.py
     |    col_vis_plan.py
     |    *.py
     +--- vis_instance
          |    interface.py
          |    basic_vis_instance.py
          |    opt_vis_instance.py
+--- tests
     |    conftest.py
     |    test_basic_complete.py
     |    test_opt_stats_complete.py
     |    test_opt_perf_complete.py
+--- test_bash_command
     |    test_basic_*.sh
     |    test_opt_*.sh
     |    test_scalability_[scale factor].py
     |    test_scalability_100_[top k plan].py
+--- utils
     |    preprocess.py
     |    plot_exp_data.py
```
`src`: This directory contains all source code, whose structure follows much of the design document. Some auxiliary classes/enums are defined to make the implementation cleaner. To be professional, `No Merge`, `Overlap Merge` , `Statistics-based Merge` , `Prune` are renamed to `basic independent`, `basic merge`, `optimized stats`, `optimized perf` (i.e. optimized performance) respectively. These changes are reflected in `src/datalake_vis/vis_instance`.

`tests`: This directory contains the unit tests for all **VisInstance**. The suffix "complete" means the test is run on all queries from a dataset. `basic independent` and `basic merge` share the same test script as they don't differ by much. More details on the tests are included in [Running Tests](#testing).

`test_bash_command`: This directory contains the commands and scripts used to conduct effectiveness and efficiency experiments.

`utils`: This directory contains data pre-processing script that (1) convert date to ordinals and (2) convert text to numerical values/embeddings. The other ploting script simply plots the performance experiment data.

## Package Management
We use [Poetry](https://python-poetry.org/) for dependencies management. The config file is `pyproject.toml`. To install poetry, run
```
pip install poetry
```
After installation, run the following commands to build the virtual environment:
```
poetry init      # initialize poetry env at project home
poetry lock      # fix dependencies
poetry install   # istall python lib in virtual environment
poetry shell     # activate the virtual environment
```
To deactivate the poetry shell, simply run
```
deactivate
```

## Testing
### Dataset setup
1. Under project home directory, create a directory named `data`.
2. Under `data` directory, create your data directories following this example:
```
data
+--- santos
     +--- datalake
          |    *.csv
     +--- query
          |    *.csv
     +--- datalake_new (optional)
          |    *.csv
     +--- query_new (optional)
          |    *.csv
     +--- tus_results
          |    results_k50_t2.pkl
     +--- results_k50_t2_[vis instance name]
          |    run.log
          |    error.log
     ...
...
```
`datalake` and `query` contains the original data. `datalake_new` and `query_new` contains the pre-processed data produced by `utils/preprocess.py`, and these two directories are optional. If you do not have pre-processed data, the running time of the framework will likely be much longer as data are processed on the fly.

`tus_results` contains the Starmie search result with column matching. **k50** means Starmie outputs the top 50 result tables and **t2** means the Starmie threshold is set to 0.2. The pickle files contains a dictionary in the following format:
```
{
    "query_table1.csv": {
        "result_table1.csv": [(0,0), (1,1), (2,2),...],
        "result_table2.csv": [(0,0), (1,1), (2,2),...],
        ...
    },
    ...
}
```
It is a mapping between query table and result tables along with column index matching.

`results_k50_t2_[vis instance name]`: these directories will be generated when running tests, and we do not need to create them manually. `run.log` contains the test statistics, `error.log` logs all exceptions encountered.


### Running test
All required parameters are specified in the `tests/conftest.py`. Here are a more detailed explanation on some of the parameters.

`vis_ins`: can be `basic_ind`, `basic_merge`, `opt_stats`, `opt_perf`.

`K`: output top k visualization plans.

`total_batch`: only used by `opt_perf`. 

`bucket_num`: number of buckets when dealing with numerical data.

`preprocessed_data`: true if we are working with pre-processed data by `preprocess.py`. This will affect how column data is handled (check out `src/datalake_vis/column.py`).

`text_format`: either `numerical` or `vector`, indicating how textual data is handled.

The default setting in the `conftest.py` is to run the tests on Santos dataset. To run the test on other query, please check out the commands under `test_bash_command`. Note that all `tests/test_*_complete.py` is a duplicate of the `find_top_k` method in their corresponding **VisInstance**. We have code duplicate here because we want to measure the running time for each step.


