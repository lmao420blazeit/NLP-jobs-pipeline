import logging 
import os
import pandas as pd
from tqdm import tqdm
import nltk
from nltk.corpus import stopwords
import string
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from prefect import task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

nltk.download('stopwords')

@task(name="Generate Tokens",
        description="Remove stopwords, remove ponctuation, assign tokens list")
def read_json(path = "data_preprocessed_02"):
    if not os.path.exists("data_preprocessed_03"):
        os.makedirs("data_preprocessed_03")

    stop = set(stopwords.words('english') + list(string.punctuation))

    for __json in tqdm(os.listdir(path)):
        logger.info(f"Reading file: {__json}")
        data = pd.read_json("data_preprocessed_01/"+__json)
        tokens_list = []
        for index, value in data["description"].items():
            tokens = [i for i in nltk.word_tokenize(value.lower()) if i not in stop]
            tokens_list.append(tokens)

        data["tokens"] = pd.Series(tokens_list)
        data.to_json("data_preprocessed_03/" + __json, orient="records")

    return 

read_json()