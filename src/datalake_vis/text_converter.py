""" Textual to numerical converter """

from abc import ABC, abstractmethod

import gensim
import numpy as np
import pandas as pd
from nltk.tokenize import sent_tokenize, word_tokenize


class Text2Type(ABC):
    """Abstract class for textual to numerical converter"""

    @staticmethod
    @abstractmethod
    def convert(data: pd.Series):
        """Convert the textual data in each cell into a representative number in place

        Args:
            data: a single column of textual data
        Returns:
            None, swap in place
        """
        raise NotImplementedError("Not implemented")


class Word2Num(Text2Type):
    """Use word2vec to convert to embedding then sum up the embedding vector"""

    @staticmethod
    def convert(data: pd.Series) -> None:
        """Convert the textual data of a column into average of its word embeddings"""
        words = []
        for index in data.index:
            cur_str = str(data[index]).replace("\n", " ")
            # iterate through each sentence in the file
            for i in sent_tokenize(cur_str):
                temp = []
                # tokenize the sentence into words
                for j in word_tokenize(i):
                    temp.append(j.lower())
                words.append(temp)

        # Create CBOW model
        model = gensim.models.Word2Vec(words, min_count=1, vector_size=100, window=5)
        for index in data.index:
            tp: np.ndarray = np.mean(
                [
                    model.wv[w.lower()]
                    for i in sent_tokenize(str(data[index]).replace("\n", " "))
                    for w in word_tokenize(i)
                ],
                axis=0,
            )
            data.at[index] = (
                tp.sum() * 1000
            )  # times 1000 to have the number large enough to be divided into integer ranges


class Word2Vec(Text2Type):
    """Use word2vec to convert to embedding then sum up the embedding vector"""

    @staticmethod
    def convert(data: pd.Series) -> None:
        """Convert the textual data of a column into average of its word embeddings"""
        words = []
        for index in data.index:
            cur_str = str(data[index]).replace("\n", " ")
            # iterate through each sentence in the file
            for i in sent_tokenize(cur_str):
                temp = []
                # tokenize the sentence into words
                for j in word_tokenize(i):
                    temp.append(j.lower())
                words.append(temp)

        # Create CBOW model
        model = gensim.models.Word2Vec(words, min_count=1, vector_size=100, window=5)
        for index in data.index:
            tp: np.ndarray = np.mean(
                [
                    model.wv[w.lower()]
                    for i in sent_tokenize(str(data[index]).replace("\n", " "))
                    for w in word_tokenize(i)
                ],
                axis=0,
            )
            data.at[index] = tp
