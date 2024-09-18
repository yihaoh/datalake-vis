"""Pytest arguments"""


def pytest_addoption(parser):
    # data path
    parser.addoption("--table_search_result_path", action="store", default="data/santos/tus_results")
    parser.addoption("--query_path", action="store", default="data/santos/query_new")
    parser.addoption("--datalake_path", action="store", default="data/santos/datalake_new")
    parser.addoption("--result_path", action="store", default="data/santos/basic_results")

    # in case we need refer back to these
    parser.addoption("--orig_query_path", action="store", default="data/santos/query")
    parser.addoption("--orig_datalake_path", action="store", default="data/santos/datalake")

    # starmie config
    parser.addoption("--threshold", action="store", default="0.2")
    parser.addoption("--N", action="store", default="10")

    # visualization recommendation config
    parser.addoption("--vis_ins", action="store", default="basic_merge")
    parser.addoption("--K", action="store", default="10")
    parser.addoption("--total_batch", action="store", default="10")
    parser.addoption("--bucket_num", action="store", default="5")
    parser.addoption("--preprocessed_data", action="store", default="True")
    parser.addoption(
        "--text_format", action="store", default="numerical"
    )  # only support text as numerical or embedding vector
