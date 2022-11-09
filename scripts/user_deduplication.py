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
from data_collection.dataaccess.user import User, schema as user_schema
from data_collection.dataaccess.tweet import Tweet


def deduplicate(
    output_path: str,
    data_base_dir: str,
    begin_date: str,
    end_date: str,
    logger=None,
):
    pass



if __name__ == '__main__':

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--output_dir', help="The path to store the extracted data as parquet files", required=True)
    arg_parser.add_argument('--data_base_dir', help="Base path to read compressed .gz files from decahose", required=True)
    arg_parser.add_argument('--begin_date', help="begin date of extraction", default='2020-05-06')
    arg_parser.add_argument('--end_date', help="end date of extraction", default='2020-05-06')
    arg_parser.add_argument('--log', type=str, help="logging file name", default='default.log')

    args = arg_parser.parse_args()

    base_dir = pathlib.Path(__file__).parent.parent.resolve()

    logging.basicConfig(
        filename=os.path.join(base_dir, 'logs', 'root.log'),
        level=logging.DEBUG,
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    )

    logger = logging.getLogger('user_deduplication')
    logger.setLevel(logging.DEBUG)

    log_handler = logging.FileHandler(os.path.join(base_dir, 'logs', args.log))
    logger.addHandler(log_handler)

    deduplicate(
        output_path=args.output_dir,
        data_base_dir=args.data_base_dir,
        begin_date=args.begin_date,
        end_date=args.end_date,
        logger=logger,
    )
