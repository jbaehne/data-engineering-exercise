import typing
import concurrent.futures
import os
import json
import time
import logging
import yaml

import requests

# break this script up into a installable python package
# base class to hold common behavior across different api collectors for open library


class BaseCollector:

    # extend this to read configs from multiplie sinks for prod as requirements dictate
    def __init__(self, config_location: str):
        self.config_location = config_location
        self.config = None
        self.logger = logging.getLogger(BaseCollector.__name__)
        self.set_config()

    def set_config(self) -> None:
        try:
            with open(self.config_location) as file_handle:
                self.config = yaml.safe_load(file_handle)
        except Exception as e:
            self.logger.error(f"error loading config at {self.config_location} with {str(e)}")
            raise e

    def get_subject(self) -> str:
        return self.config["collector"]["subject"]

    def get_output_path(self) -> str:
        return self.config["collector"]["output_location"]

    def get_url(self) -> str:
        base_url: str = self.config["collector"]["base_url"]
        subject: str = self.config["collector"]["subject"]

        return f"{base_url}/{subject}.json"

    def get_threads(self) -> int:
        return self.config["collector"]["threads"]

    def get_total_records(self):
        try:
            return requests.get(f"{self.get_url()}?offset=0&limit=1").json()['work_count']
        except Exception as e:
            self.logger.error(f"unable to get total records for creating thread batches with error {e}")
            raise e

    @staticmethod
    def get_data_pages(total_recs: int, recs_per_batch: int = 1000) -> typing.Iterable:
        batches_required = total_recs // recs_per_batch + 1
        batches = range(0, batches_required)

        return map(lambda val: {
            'start': val * recs_per_batch,
            'end': (val * recs_per_batch) + recs_per_batch,
            'limit': recs_per_batch
        }, batches)

    def create_file_name(self, start: int, end: int):
        file_ts = str(int(time.time() * 1000))
        return f"{self.get_subject()}-{file_ts}-{start}-{end}.json"

    def collect(self):
        pass


class SubjectCollector(BaseCollector):

    def write_data(self, config: dict, data: typing.List[dict]):
        file_name = self.create_file_name(config['start'], config['end'])
        path = os.path.join(self.get_output_path(), file_name)
        self.logger.info(f"writing {len(data)} to {path}")

        with open(path, 'w') as write_file:
            for rec in data:
                write_file.write(json.dumps(rec) + "\n")

    def get_data(self, config: dict, retry_amt: int = 0, retry_limit: int = 3):
        url = f"{self.get_url()}?offset={config['start']}&limit={config['limit']}"
        try:
            data = requests.get(url).json()
        except Exception as e:
            self.logger.error(f"failed to get data for {url} with config: {config}")
            retry_amt += 1

            if retry_amt >= retry_limit:
                raise e
            else:
                self.get_data(config, retry_amt, retry_limit)
        finally:
            self.write_data(config, data.get("works", []))

    def collect(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.get_threads()) as workers:
            tasks = {
                workers.submit(self.get_data, config): i
                for i, config in enumerate(self.get_data_pages(self.get_total_records()))
            }

            for future in concurrent.futures.as_completed(tasks):
                task = tasks[future]
                try:
                    future.result()
                except Exception as exc:
                    print(f"task {task} exception: {exc}")
                else:
                    print(f"task {task} completed")


if __name__ == "__main__":
    c = SubjectCollector("/opt/spark/work-dir/collector/fantasy_conf.yaml")
    c.collect()
