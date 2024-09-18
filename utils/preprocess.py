import argparse
import os
import pickle

import pandas as pd
from dateutil.parser import parse

from datalake_vis.data_types import DataType
from datalake_vis.text_converter import Word2Num
from datalake_vis.utils import is_date


def preprocess(src_dir: str, res_dir: str):
    os.makedirs(res_dir, exist_ok=True)
    meta = {}
    for filename in os.listdir(src_dir):
        f = os.path.join(src_dir, filename)
        f_res = os.path.join(res_dir, filename)
        print(f"src: {f}")
        print(f"res: {f_res}")
        df = pd.read_csv(f)
        is_dates = []
        col_types = []
        for series_name, series in df.items():
            is_dates.append(is_date(series))
            col_types.append(DataType.check_type(series))
            last = None
            if is_dates[-1]:
                for idx in series.index:
                    try:
                        series.at[idx] = parse(series[idx]).toordinal()
                        last = series[idx]
                    except Exception:
                        series.at[idx] = last
            if col_types[-1] == DataType.TEXTUAL:
                Word2Num.convert(series)
            print(f"Finish column: {series_name}")
        print("=============================")
        meta[filename] = (col_types, is_dates)
        df.to_csv(f_res, index=False)
    pickle.dump(meta, open(f"{res_dir}/meta.pkl", "wb"))


def preprocess_with_new_col_name(src_dir: str, res_dir: str):
    os.makedirs(res_dir, exist_ok=True)
    for filename in os.listdir(src_dir):
        f = os.path.join(src_dir, filename)
        f_res = os.path.join(res_dir, filename)
        print(f"src: {f}")
        print(f"res: {f_res}")
        df = pd.read_csv(f, lineterminator="\n")
        new_col_names = []
        for series_name, series in df.items():
            try:
                is_dates = is_date(series)
                col_type = DataType.check_type(series)
            except Exception:
                new_col_names.append(f"{series_name}@(textual,{False})")
                continue
            last = None
            if is_dates:
                for idx in series.index:
                    try:
                        series.at[idx] = parse(series[idx]).toordinal()
                        last = series[idx]
                    except Exception:
                        series.at[idx] = last
            if col_type == DataType.TEXTUAL:
                try:
                    Word2Num.convert(series)
                except Exception:
                    pass
            new_col_name = f"{series_name}@({col_type.value},{is_dates})"
            new_col_names.append(new_col_name)
            print(f"Finish column: {series_name}, {new_col_name}")
        df.columns = new_col_names
        print("=============================")
        df.to_csv(f_res, index=False)


def preprocess_file(filename: str, target: str):
    df = pd.read_csv(filename)
    new_col_names = []
    for series_name, series in df.items():
        is_dates = is_date(series)
        col_type = DataType.check_type(series)
        last = None
        if is_dates:
            for idx in series.index:
                try:
                    series.at[idx] = parse(series[idx]).toordinal()
                    last = series[idx]
                except Exception:
                    series.at[idx] = last
        if col_type == DataType.TEXTUAL:
            Word2Num.convert(series)
        new_col_name = f"{series_name}@({col_type.value},{is_dates})"
        new_col_names.append(new_col_name)
        print(f"Finish column: {series_name}, {new_col_name}")
    df.columns = new_col_names
    print("=============================")
    print(df.head())
    df.to_csv(target, index=False)


def clean_up_index(src_dir: str):
    for filename in os.listdir(src_dir):
        f = os.path.join(src_dir, filename)
        print(f"src: {f}")
        df = pd.read_csv(f, index_col=0)
        df.to_csv(f, index=False)


if __name__ == "__main__":
    dl_dir = "data/santos/datalake"
    q_dir = "data/santos/query"
    dl_res_dir = "data/santos/datalake_new"
    q_res_dir = "data/santos/query_new"

    parser = argparse.ArgumentParser()
    parser.add_argument("--src", type=str, default="data/santos/query")
    parser.add_argument("--tar", type=str, default="data/santos/query_new")
    hp = parser.parse_args()

    # preprocess_with_new_col_name(q_dir, q_res_dir)
    # print("=================================="
    # preprocess_with_new_col_name(dl_dir, dl_res_dir)
    # preprocess_with_new_col_name("data/tus/datalake", "data/tus/datalake_new")
    # python utils/preprocess.py --src data/tus/datalake --tar data/tus/datalake_new
    preprocess_with_new_col_name(hp.src, hp.tar)
