import requests
import json
import logging
import time
import os
from backoff import on_exception, expo
from airflow.hooks.base import BaseHook
from prefect import task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ArbeitnowHook(BaseHook):
    """
    Hook into data stream/batch through Arbeitnow API
    """

    def __init__(self) -> None:
        self.initial_url = 'https://www.arbeitnow.com/api/job-board-api?page=1'
        self.headers = {}

    @on_exception(expo, requests.exceptions.HTTPError, max_tries=3)
    @task(name="Fetch Data",
          description="Fetch all data from Arbeitnow Endpoint")
    def get_request(self, url = "https://www.arbeitnow.com/api/job-board-api?page=1", sleep = 2) -> json:
        """
        Get requests from arbeitnow API and dump it into JSON
        """
        time.sleep(sleep)
        response = requests.request("GET", url=url, headers=self.headers)
        logger.info(f"Getting data from endpoint: {url}")
        logger.info(f"Status code: {response.status_code}")
        if response.status_code != 200:
            raise Exception(response.status_code, response.text)

        if not os.path.exists("data"):
            os.makedirs("data")

        json_path = 'data/file-{}.json'.format(url[-6:])
        with open(json_path, 'w') as f:
            logger.info(f"Dumping json: {json_path} -> {url}")
            json.dump(response.json()["data"], f)

        next = response.json()["links"]["next"]
        return (next)
    
    def get_all(self):
        next = self.get_request()
        while True:
            if next == None:
                logger.info(f"Reach the end of the pages: {next}")
                break
            next = self.get_request(url = next)     
        return  
    
if __name__ == "__main__":
    Fetch = ArbeitnowHook()
    next = Fetch.get_all()

