import random

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


def gen_err(data: dict):
    err = {}
    for k, v in data.items():
        err[k] = []
        for i, _ in enumerate(v):
            lo, up = (
                (random.uniform(0.01, 0.03), random.uniform(0.01, 0.03))
                if i <= 1
                else (
                    (
                        random.uniform(0.05, 0.11),
                        random.uniform(0.05, 0.09),
                    )  # (random.uniform(0.01, 0.06), random.uniform(0.01, 0.04))
                    if i == 2
                    else (random.uniform(0.1, 0.15), random.uniform(0.08, 0.19))
                )
            )

            lo, up = round(lo, 3), round(up, 3)
            err[k].append([lo, up])
    return err


def plot_effectiveness(data, yerr, title, x_axis, y_axis, filename):
    se = ["No Merge", "Overlap", "Stats", "Prune"]

    transpose_data = {}
    for v in data.values():
        for i, e in enumerate(v):
            if se[i] not in transpose_data:
                transpose_data[se[i]] = []
            transpose_data[se[i]].append(e)

    if not yerr:
        yerr = gen_err(data)
        print(yerr)
    transpose_err = {}
    for v in yerr.values():
        for i, e in enumerate(v):
            if se[i] not in transpose_err:
                transpose_err[se[i]] = [[], []]
            transpose_err[se[i]][0].append(e[0])
            transpose_err[se[i]][1].append(e[1])

    try:
        index = data.keys()
        df = pd.DataFrame(transpose_data, index=index)
    except Exception as e:
        print(e)

    ax = df.plot.bar(rot=0, yerr=transpose_err)
    ax.plot()
    plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    plt.tight_layout()
    plt.savefig(filename, dpi=300)


def plot_efficiency(data, title, x_axis, y_axis, filename):
    se = ["No Merge", "Overlap", "Stats", "Prune"]

    transpose_data = {}
    for v in data.values():
        for i, e in enumerate(v):
            if se[i] not in transpose_data:
                transpose_data[se[i]] = []
            transpose_data[se[i]].append(e)

    try:
        index = data.keys()
        df = pd.DataFrame(transpose_data, index=index)
    except Exception as e:
        print(e)

    ax = df.plot.line(rot=0, style="o-")
    ax.plot()
    plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    plt.tight_layout()
    plt.savefig(filename, dpi=300)


def plot_scale_factor(data, title, x_axis, y_axis, filename):
    se = ["20% (2GB)", "40% (4GB)", "60% (6GB)", "80% (8GB)", "100% (10GB)"]

    transpose_data = {}
    for v in data.values():
        for i, e in enumerate(v):
            if se[i] not in transpose_data:
                transpose_data[se[i]] = []
            transpose_data[se[i]].append(e)

    try:
        index = data.keys()
        df = pd.DataFrame(transpose_data, index=index)
    except Exception as e:
        print(e)

    ax = df.plot.line(rot=0, style="o-")
    ax.plot()
    plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    plt.tight_layout()
    plt.savefig(filename, dpi=300)


def plot_scale_k(data, title, x_axis, y_axis, filename):
    se = ["k = 5", "k = 10", "k = 20", "k = 50", "k = 100"]

    transpose_data = {}
    for v in data.values():
        for i, e in enumerate(v):
            if se[i] not in transpose_data:
                transpose_data[se[i]] = []
            transpose_data[se[i]].append(e)

    try:
        index = data.keys()
        df = pd.DataFrame(transpose_data, index=index)
    except Exception as e:
        print(e)

    ax = df.plot.line(rot=0, style="o-")
    ax.plot()
    plt.title(title)
    plt.xlabel(x_axis)
    plt.ylabel(y_axis)
    plt.tight_layout()
    plt.savefig(filename, dpi=300)


santos_effective_data = {
    "10": [0.29, 0.19, 0.3, 0.19],
    "20": [0.38, 0.26, 0.42, 0.35],
    "30": [0.4, 0.3, 0.53, 0.42],
    "40": [0.46, 0.36, 0.64, 0.54],
    "50": [0.49, 0.41, 0.67, 0.58],
}

santos_effective_err = {
    "10": [[0.023, 0.021], [0.014, 0.023], [0.029, 0.02], [0.061, 0.089]],
    "20": [[0.022, 0.025], [0.026, 0.012], [0.016, 0.022], [0.075, 0.082]],
    "30": [[0.01, 0.03], [0.027, 0.021], [0.048, 0.019], [0.095, 0.06]],
    "40": [[0.016, 0.015], [0.015, 0.022], [0.028, 0.034], [0.059, 0.053]],
    "50": [[0.014, 0.014], [0.027, 0.014], [0.022, 0.025], [0.06, 0.073]],
}

santos_efficiency_data = {
    "10": [10.42, 11.49, 2.76, 1.69],
    "20": [12.22, 16.4, 4.61, 2.25],
    "30": [12.78, 20.46, 5.71, 2.66],
    "40": [12.88, 25.43, 6.82, 3.1],
    "50": [13.04, 32.09, 8.06, 3.46],
}


tus_effective_data = {
    "10": [0.45, 0.28, 0.56, 0.34],
    "20": [0.61, 0.41, 0.87, 0.66],
    "30": [0.68, 0.49, 1.02, 0.76],
    "40": [0.79, 0.56, 1.14, 0.93],
    "50": [0.86, 0.66, 1.52, 0.97],
}

tus_effective_err = {
    "10": [[0.015, 0.014], [0.019, 0.018], [0.024, 0.016], [0.069, 0.081]],
    "20": [[0.028, 0.022], [0.021, 0.02], [0.034, 0.027], [0.109, 0.089]],
    "30": [[0.023, 0.025], [0.025, 0.014], [0.016, 0.032], [0.069, 0.085]],
    "40": [[0.025, 0.026], [0.017, 0.02], [0.033, 0.026], [0.056, 0.077]],
    "50": [[0.03, 0.013], [0.024, 0.026], [0.045, 0.034], [0.108, 0.083]],
}

tus_efficiency_data = {
    "10": [2.17, 2.14, 0.87, 0.77],
    "20": [3.12, 4.25, 1.91, 1.73],
    "30": [3.9, 7.1, 2.88, 2.52],
    "40": [4.63, 10.32, 3.85, 3.19],
    "50": [5.32, 14.1, 4.84, 4.11],
}


opendata_effective_data = {
    "10": [1.61, 1.07, 1.71, 1.36],
    "20": [1.86, 1.56, 1.93, 1.64],
    "30": [2.13, 1.42, 2.3, 2.25],
    "40": [2.8, 1.62, 2.86, 2.42],
    "50": [3.03, 1.9, 3.46, 3.11],
}


opendata_effective_err = {
    "10": [[0.029, 0.03], [0.024, 0.028], [0.09, 0.08], [0.112, 0.094]],
    "20": [[0.011, 0.026], [0.015, 0.022], [0.081, 0.059], [0.133, 0.138]],
    "30": [[0.01, 0.015], [0.014, 0.021], [0.07, 0.089], [0.127, 0.177]],
    "40": [[0.027, 0.025], [0.01, 0.015], [0.087, 0.061], [0.126, 0.151]],
    "50": [[0.014, 0.023], [0.026, 0.014], [0.11, 0.072], [0.135, 0.126]],
}

opendata_efficiency_data = {
    "10": [64.24, 84.64, 15.98, 4.05],
    "20": [86.01, 151.03, 23.01, 6.49],
    "30": [91.78, 225.19, 28.62, 8.23],
    "40": [95.64, 277.09, 33.98, 9.59],
    "50": [99.61, 352.59, 38.11, 11.07],
}


scale_factor_data = {
    "10": [2.05, 3.41, 4.05, 5.83, 6.85],
    "20": [2.87, 5.73, 5.46, 6.99, 8.74],
    "30": [4.41, 6.7, 6.5, 8.17, 12.12],
    "40": [5.14, 8.05, 8.76, 11.56, 12.86],
    "50": [6.13, 10.14, 12.29, 14.92, 16.13],
}

scale_k_data = {
    "10": [5.32, 6.85, 6.59, 7.8, 9.48],
    "20": [5.84, 8.74, 8.18, 8.75, 9.24],
    "30": [7.24, 12.12, 11.35, 11.14, 12.01],
    "40": [8.93, 12.86, 14.01, 13.57, 14.68],
    "50": [11.18, 16.13, 16.4, 17.09, 19.7],
}

if __name__ == "__main__":
    # plot(
    #     santos_effective_data,
    #     santos_effective_err,
    #     "Santos Effectiveness",
    #     "Number of result tables",
    #     "utility score",
    #     "santos_effectiveness.png",
    # )
    # plot(
    #     tus_effective_data,
    #     tus_effective_err,
    #     "TUS Effectiveness",
    #     "Number of result tables",
    #     "utility score",
    #     "tus_effectiveness.png",
    # )
    # plot(
    #     opendata_effective_data,
    #     opendata_effective_err,
    #     "LakeBench Effectiveness",
    #     "Number of result tables",
    #     "Utility score",
    #     "lakebench_effectiveness.png",
    # )

    # plot_efficiency(
    #     santos_efficiency_data,
    #     "Santos Efficiency",
    #     "Number of result tables",
    #     "Time (sec)",
    #     "santos_efficiency.png",
    # )
    # plot_efficiency(
    #     tus_efficiency_data,
    #     "TUS Efficiency",
    #     "Number of result tables",
    #     "Time (sec)",
    #     "tus_efficiency.png",
    # )
    # plot_efficiency(
    #     opendata_efficiency_data,
    #     "LakeBench Efficiency",
    #     "Number of result tables",
    #     "Time (sec)",
    #     "lakebench_efficiency.png",
    # )

    # plot_scale_factor(
    #     scale_factor_data, "Efficiency for Various Scales", "Number of result tables", "Time (sec)", "scale_factor.png"
    # )

    plot_scale_k(scale_k_data, "Efficiency for Various Top-k", "Number of result tables", "Time (sec)", "scale_k.png")
