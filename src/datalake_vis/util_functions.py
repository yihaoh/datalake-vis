""" Abstract class for utility functions """

from abc import ABC

import numpy as np

# from pyemd import emd
from scipy.stats import wasserstein_distance


# pylint: disable-msg=W0107
# pylint: disable-msg=C0200
class UtilityFunction(ABC):
    """
    Abstract class for utility functions
    """

    # @abstractmethod
    @staticmethod
    def compute_score(data):
        """To be implemented"""
        raise NotImplementedError("Not implemented for this parent class.")


class EMD(UtilityFunction):
    """
    Earth Mover's Distance
    """

    @staticmethod
    def compute_score(data: "dict[str, list[float]]"):
        tp = {}
        for v in data.values():
            for i, e in enumerate(v):
                if i not in tp:
                    tp[i] = []
                tp[i].append(e)

        cum_emd = 0
        cnt = 0
        vals = list(tp.values())
        for i in range(len(vals)):
            arr1 = np.array(vals[i])
            arr1[np.isnan(arr1)] = 0
            if arr1.min() < 0:
                arr1 = arr1 - arr1.min()
            if not np.any(arr1):
                continue
            for j in range(i, len(vals)):
                arr2 = np.array(vals[j])
                arr2[np.isnan(arr2)] = 0
                if arr2.min() < 0:
                    arr2 = arr2 - arr2.min()
                if not np.any(arr2):
                    continue
                cur_emd = wasserstein_distance(np.arange(len(data)), np.arange(len(data)), arr1, arr2)
                cum_emd = (cum_emd * cnt + cur_emd) / (cnt + 1)
                cnt += 1
        return cum_emd if cnt > 0 else 0.0


# The following is the version with uniform distance matrix
# class EMD(UtilityFunction):
#     """
#     Earth Mover's Distance
#     """

#     @staticmethod
#     def compute_score(data: "dict[str, list[float]]") -> float:

#         tp = {}
#         for v in data.values():
#             for i, e in enumerate(v):
#                 if i not in tp:
#                     tp[i] = []
#                 tp[i].append(e)

#         cum_emd = 0
#         cnt = 0
#         vals = list(tp.values())
#         flow_matrix = np.ones((len(data), len(data)), dtype=float)
#         np.fill_diagonal(flow_matrix, 0.0)

#         for i in range(len(vals)):
#             arr1 = np.array(vals[i])
#             arr1[np.isnan(arr1)] = 0
#             if arr1.min() < 0:
#                 arr1 = arr1 - arr1.min()
#             if not np.any(arr1):
#                 continue
#             for j in range(i, len(vals)):
#                 arr2 = np.array(vals[j])
#                 arr2[np.isnan(arr2)] = 0
#                 if arr2.min() < 0:
#                     arr2 = arr2 - arr2.min()
#                 if not np.any(arr2):
#                     continue
#                 cum_emd += emd(arr1 / arr1.sum(), arr2 / arr2.sum(), flow_matrix)
#                 cnt += 1
#         return cum_emd / cnt if cnt > 0 else 0.0
