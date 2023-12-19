import logging 
import os
import pandas as pd
from tqdm import tqdm
import string
from prefect import task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def read_json(path = "data_preprocessed_03"):
    appended_data = []
    for __json in tqdm(os.listdir(path)):
        logger.info(f"Reading file: {__json}")
        appended_data.append(pd.read_json("data_preprocessed_03/"+__json))

    appended_data = pd.concat(appended_data)
    return (appended_data)

data = read_json()