import logging 
from jobs_parser import read_json as first_pass
from jobs_translator import read_json as second_pass
from jobsapi_hook import ArbeitnowHook
from prefect import flow

LOG_FILENAME = 'debug.log'
logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG)

@flow(name="Staging", log_prints=True)
def run():
    ArbeitnowHook().get_all()
    first_pass()
    second_pass()


if __name__ == "__main__":
    run.serve(name="my-first-deployment")

