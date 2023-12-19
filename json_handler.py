import logging 
import os
import pandas as pd
from tqdm import tqdm

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def read_json(path = "data_preprocessed_03"):
    for __json in tqdm(os.listdir(path)):
        logger.info(f"Reading file: {__json}")
        df = pd.read_json("data/"+__json)
        print(df)
        break

if __name__ == "__main__":
    read_json()