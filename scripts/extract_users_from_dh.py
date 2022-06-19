from tqdm import tqdm
import argparse
import json
import lzma
import os
from datetime import timedelta
from random import random
from typing import Dict

import pyspark.sql.functions as F
from dateutil import parser

from data_collection.util.spark import get_spark
from data_collection.dataaccess.user import User


def is_valid_usr(user: User, collected_users: Dict) -> bool:
    if user.uid in collected_users:
        return False

    #TODO: fix a good set of filters
    if user.description is None or len(user.description) < 3:
        return False

    return True


def extract_users(
    output_path: str,
    data_base_dir: str,
    sample_rate: float,
    user_per_day: int,
    begin_date: str,
    end_date: str
):
    current_dt = parser.parse(begin_date)
    end_dt = parser.parse(end_date)

    while current_dt < end_dt:
        current_date = current_dt.date()
        path = os.path.join(data_base_dir, f"tweets.json.{str(current_date)}.xz")
        n_users = 0
        users = {}

        with lzma.open(path, 'r') as f:
            content = f.readline().decode('utf-8')

            with tqdm(total=user_per_day) as progress_bar:
                while len(content) > 0 and n_users < user_per_day:
                    if random() < sample_rate:
                        json_d = json.loads(content)
                        user = User(json_d['user'])
                        if is_valid_usr(user, users):
                            users[user.uid] = user
                            n_users += 1
                            progress_bar.update(1)

                    content = f.readline().decode('utf-8')

        #TODO: store in spark format
        print(len(users))
        current_dt = current_dt + timedelta(days=1)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--output_dir', help="The path to store the §extracted data as parquet files", required=True)
    arg_parser.add_argument('--data_base_dir', help="Base path to read compressed .gz files from decahose", required=True)
    arg_parser.add_argument('--begin_date', help="begin date of extraction", default='2020-05-06')
    arg_parser.add_argument('--end_date', help="end date of extraction", default='2020-05-06')
    arg_parser.add_argument('--sample_rate', type=float, help="The rate of sampling tweets", default=0.001)
    arg_parser.add_argument('--user_per_day', type=int, help="number of new users to look at per day", default=10000)
    args = arg_parser.parse_args()

    extract_users(
        output_path=args.output_dir,
        data_base_dir=args.data_base_dir,
        sample_rate=args.sample_rate,
        user_per_day=args.user_per_day,
        begin_date=args.begin_date,
        end_date=args.end_date
    )
