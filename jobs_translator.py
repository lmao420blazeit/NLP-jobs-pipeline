import logging 
import os
import pandas as pd
from deep_translator.exceptions import NotValidLength
from deep_translator import GoogleTranslator
from tqdm import tqdm
from prefect import task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from translate import Translator

@task(name="Translate description",
        description="Translate description to english when language detected != eng")
def read_json(path = "data_preprocessed_01"):
    if not os.path.exists("data_preprocessed_02"):
        os.makedirs("data_preprocessed_02")

    for __json in tqdm(os.listdir(path)):
        logger.info(f"Reading file: {__json}")
        df = pd.read_json("data_preprocessed_01/"+__json)
        df = translate(df)
        df.to_json("data_preprocessed_02/" + __json, orient="records")
    return 

def translate(data):
    translator = GoogleTranslator(source="auto", target="en")
    for index, value in data["original_language"].items():
        if value not in ["eng ", "eng"]: # this is retarded; can be both values
            data.at[index, "title"] = translator.translate(data.iloc[index]["title"])

            try:
                data.at[index, "description"] = translator.translate(data.iloc[index]["description"])

            except NotValidLength:
                data.at[index, "description"] = translator.translate(data.iloc[index]["description"][0:4999])
                print(data.at[index, "description"])
                logger.info(f"{NotValidLength} on index {index}")

    return (data)

if __name__ == "__main__":
    read_json()