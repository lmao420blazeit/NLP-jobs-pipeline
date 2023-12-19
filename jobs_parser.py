import logging 
import os
import pandas as pd
from bs4 import BeautifulSoup
import nltk
from unicodedata import normalize
from tqdm import tqdm
import regex as re
from prefect import task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from translate import Translator

@task(name="Initial parser",
        description="Remove HTML tags, remove unicode, remove, assign original_language field")
def read_json(path = "data"):
    if not os.path.exists("data_preprocessed_01"):
        os.makedirs("data_preprocessed_01")

    for __json in tqdm(os.listdir(path)):
        logger.info(f"Reading file: {__json}")
        print("data/"+__json)
        df = pd.read_json("data/"+__json)
        df = remove_html_tags(df)
        df = remove_tags_slug(df)
        df.to_json("data_preprocessed_01/" + __json, orient="records")

    return 

def remove_html_tags(data):
    """
    Removes HTML tags from text
    Removes line breaks from text
    Removes unicode variables

    + detects language from sample text
    + creates an additional field: [original_language]
    
    Updates the text according to the new changes 

    """
    lang_list = []
    for index, value in data["description"].items():
        __text = BeautifulSoup(value, "lxml").text.replace('\r', '').replace('\n', '')
        data["description"][index] = __text.encode("ascii", 'ignore').decode('ascii')
        lang = detect_language(data["description"][index][0:130])
        
        lang_list.append(lang)

    data["original_language"] = pd.Series(lang_list)

    return (data)

def remove_tags_slug(data):
    """
    Removes "-" tags and number fields

    kaufmannischer-mitarbeiter-hofgeismar-154258 -> kaufmannischer mitarbeiter hofgeismar
    """
    for index, value in data["slug"].items():
        __text = value.replace('-', ' ')
        __text = re.sub(r"\b\d+\b", "", __text)
        data["slug"][index] = __text

    return (data)


def detect_language(data):
    """
    Returns the language from the job offer
    """
    tc = nltk.classify.textcat.TextCat() 
    return tc.guess_language(data)


if __name__ == "__main__":
    read_json()