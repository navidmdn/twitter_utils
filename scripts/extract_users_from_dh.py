from tqdm import tqdm
import argparse
import json
import lzma
import os
import logging
from datetime import timedelta
from random import random
from typing import Dict
import pathlib
from time import time

import pyspark.sql.functions as F
from dateutil import parser

from data_collection.util.spark import get_spark
from data_collection.dataaccess.user import User


def is_valid_usr(user: User, lang: str, collected_users: Dict) -> bool:
    if user.uid in collected_users:
        return False

    #TODO: fix a good set of filters
    if user.description is None or len(user.description) < 3:
        return False

    if lang not in ['en', 'und']:
        return False

    return True


def extract_users(
    output_path: str,
    data_base_dir: str,
    sample_rate: float,
    user_per_day: int,
    begin_date: str,
    end_date: str,
    logger=None
):
    current_dt = parser.parse(begin_date)
    end_dt = parser.parse(end_date)
    spark = get_spark(driver_mem=20, cores=30)

    logger.info(f"Extracting users from {begin_date} to {end_date}")

    while current_dt < end_dt:
        begin_t = time()

        current_date = current_dt.date()
        path = os.path.join(data_base_dir, f"tweets.json.{str(current_date)}.xz")
        n_users = 0
        users = {}

        with lzma.open(path, 'r') as f:
            content = f.readline().decode('utf-8')

            i = 1
            while len(content) > 0 and n_users < user_per_day:
                try:
                    if i % 1000000 == 0:
                        logger.info(f"processed {i} records")
                    if random() < sample_rate:
                        json_d = json.loads(content)
                        user = User(json_d['user'])
                        user.set_record_time(json_d['created_at'])
                        if is_valid_usr(user, json_d['lang'], users):
                            users[user.uid] = user
                            n_users += 1
                except Exception as e:
                    logger.error(f"couldn't parse line with error: {e}")

                content = f.readline().decode('utf-8')
                i += 1

        logger.info(f"storing parquet file for {current_date}")
        logger.info(f"found {len(users)} distinct users")
        users_list = list(users.values())
        df = spark.createDataFrame(users_list)
        output_file_path = os.path.join(output_path, f'{current_date}.parquet')
        df.write.parquet(output_file_path, mode='overwrite')
        logger.info(f"{current_date} finished in {time()-begin_t}s")

        current_dt = current_dt + timedelta(days=1)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--output_dir', help="The path to store the §extracted data as parquet files", required=True)
    arg_parser.add_argument('--data_base_dir', help="Base path to read compressed .gz files from decahose", required=True)
    arg_parser.add_argument('--begin_date', help="begin date of extraction", default='2020-05-06')
    arg_parser.add_argument('--end_date', help="end date of extraction", default='2020-05-06')
    arg_parser.add_argument('--sample_rate', type=float, help="The rate of sampling tweets", default=1.0)
    arg_parser.add_argument('--user_per_day', type=int, help="number of new users to look at per day", default=10000000)
    arg_parser.add_argument('--log', type=str, help="logging file name", default='default.log')
    args = arg_parser.parse_args()

    base_dir = pathlib.Path(__file__).parent.parent.resolve()

    logging.basicConfig(
        filename=os.path.join(base_dir, 'logs', 'root.log'),
        level=logging.DEBUG,
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    )

    logger = logging.getLogger('extract_users')
    logger.setLevel(logging.INFO)
    log_handler = logging.FileHandler(os.path.join(base_dir, 'logs', args.log))
    logger.addHandler(log_handler)

    extract_users(
        output_path=args.output_dir,
        data_base_dir=args.data_base_dir,
        sample_rate=args.sample_rate,
        user_per_day=args.user_per_day,
        begin_date=args.begin_date,
        end_date=args.end_date,
        logger=logger
    )
