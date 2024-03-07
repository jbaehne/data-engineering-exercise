import requests
import pandas as pd
from pandas import DataFrame
from typing import Union
import datetime
import json
import os
import structlog

# extract data from library API
# focus on a single subject
SUBJECT = "love"
URL = f"http://openlibrary.org/subjects/{SUBJECT}.json?published_in=1500-1600?details=true"
GOOD_RESPONSE_CODE = 200


def get_data(url: str) -> dict:
    # request data from api
    payload = requests.get(url, timeout=10)
    if payload.status_code == GOOD_RESPONSE_CODE:
        # get json formatted data
        payload_json = payload.json()
        return payload_json
    else:
        print("something went wrong")
        data = payload.text
        raise Exception(f"Something went wrong: see api response: \n {data}")


def create_filename(
    subject: str,
    filename_prefix: str = "subject_api_extract",
    file_format: str = "json",
) -> str:
    """
    Creates the name of the file the data will be saved in.
    The finished file name would look like `subject_api_extract__love__2024_03_07_08_40_08.json`
    """
    extracted_datetime = datetime.datetime.now()
    extracted_dateteime_fmt = extracted_datetime.strftime("%Y_%m_%d_%H_%M_%S")

    filename = f"{filename_prefix}__{subject}__{extracted_dateteime_fmt}.{file_format}"
    return filename


def save_api_results(filename: str, api_data: dict, file_dir: Union[str, None]) -> None:
    """Save results
    Save the results to the desired folder
    """
    if file_dir is None:
        raise Exception("No destination directory provided")

    # save file to disk
    with open(f"{file_dir}/{filename}", "w", encoding="utf-8") as f:
        f.write(json.dumps(api_data))


def create_dir(dir_: str) -> None:
    """creates a directory"""
    if not os.path.exists(dir_):
        os.mkdir(dir_)


def load_to_db(api_data: dict) -> None:
    """Loads data to db
    This is a place holder function to save time.
    """
    print("loaded to db!")


def main():
    create_dir("raw")
    api_data = get_data(URL)
    filename = create_filename(SUBJECT)
    save_api_results(filename=filename, api_data=api_data, file_dir="raw")
    load_to_db(api_data=api_data)


if __name__ == "__main__":
    main()
