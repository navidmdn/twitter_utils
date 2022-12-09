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


def process_tweets_and_users(date, data_base_dir, lang, batch_size=1000000):
    tweets_path = os.path.join(data_base_dir, f"tweets.json.{str(date)}.xz")

    users = {}
    tweets = {}

    with lzma.open(tweets_path, 'r') as f:
        content = f.readline().decode('utf-8')

        i = 1
        while len(content) > 0:
            try:
                if i % batch_size == 0:
                    logger.debug(f"processed {i} records")
                    yield users, tweets
                    users = {}
                    tweets = {}

                json_d = json.loads(content)
                user = User(json_d['user'])
                tweet = Tweet(json_d)

                if tweet.lang != 'all':
                    if tweet.lang not in ['und', lang]:
                        content = f.readline().decode('utf-8')
                        i += 1
                        continue

                user.set_record_time(json_d['created_at'])

                # in order to keep distinct user description pairs in each day
                users[(user.uid, user.description)] = user
                tweets[tweet.id] = tweet

                #extracting both user and tweet of the referenced tweet
                if tweet.retweeted_id is not None:
                    try:
                        retweeted_usr = User(json_d['retweeted_status']['user'])
                        retweeted_tweet = Tweet(json_d['retweeted_status'])
                        tweets[retweeted_tweet.id] = retweeted_tweet
                        retweeted_usr.set_record_time(json_d['created_at'])
                        users[(retweeted_usr.uid, retweeted_usr.description)] = retweeted_usr
                    except Exception as e:
                        logger.error(f"could not extract retweeted user: {e}")

                if tweet.quoted_id is not None:
                    try:
                        quoted_usr = User(json_d['quoted_status']['user'])
                        quoted_usr.set_record_time(json_d['created_at'])
                        retweeted_tweet = Tweet(json_d['quoted_status'])
                        tweets[retweeted_tweet.id] = retweeted_tweet
                        users[(quoted_usr.uid, quoted_usr.description)] = quoted_usr
                    except Exception as e:
                        logger.error(f"could not extract quoted user: {e}")

            except Exception as e:
                logger.error(f"couldn't process tweet/user record with error: {e}")

            content = f.readline().decode('utf-8')
            i += 1

    yield users, tweets


def extract_tables(
    output_path: str,
    data_base_dir: str,
    begin_date: str,
    end_date: str,
    lang: str,
    logger=None,
):
    current_dt = parser.parse(begin_date)
    end_dt = parser.parse(end_date)
    spark = get_spark(driver_mem=30, cores=30)

    logger.info(f"Extracting tables from {begin_date} to {end_date}")

    while current_dt < end_dt:
        begin_t = time()
        current_date = current_dt.date()
        try:
            for batch_no, (users, tweets) in enumerate(process_tweets_and_users(current_date, data_base_dir, lang)):

                logger.debug(f"storing users and tweets parquet file for {current_date}")

                users_list = list(users.values())
                users_df = spark.createDataFrame(users_list, schema=user_schema)

                tweets_list = list(tweets.values())
                tweets_df = spark.createDataFrame(tweets_list)

                output_users_file_path = os.path.join(output_path, 'users', f'date={current_date}', f'{batch_no}.parquet')
                output_tweets_file_path = os.path.join(output_path, 'tweets', f'date={current_date}', f'{batch_no}.parquet')

                tweets_df.write.parquet(output_tweets_file_path, mode='overwrite')
                users_df.write.parquet(output_users_file_path, mode='overwrite')

            logger.info(f"users and tweets for {current_date} finished in {time()-begin_t}s")
        except Exception as e:
            logger.exception(f"Could not process the file in {current_date} due to: ", e)

        current_dt = current_dt + timedelta(days=1)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--output_dir', help="The path to store the extracted data as parquet files", required=True)
    arg_parser.add_argument('--data_base_dir', help="Base path to read compressed .gz files from decahose", required=True)
    arg_parser.add_argument('--begin_date', help="begin date of extraction", default='2020-05-06')
    arg_parser.add_argument('--end_date', help="end date of extraction", default='2020-05-06')
    arg_parser.add_argument('--log', type=str, help="logging file name", default='default.log')
    arg_parser.add_argument('--lang', type=str, help="language to filter", default='all')

    args = arg_parser.parse_args()

    base_dir = pathlib.Path(__file__).parent.parent.resolve()

    logging.basicConfig(
        filename=os.path.join(base_dir, 'logs', 'root.log'),
        level=logging.DEBUG,
        format='%(asctime)s:%(levelname)s:%(name)s:%(message)s'
    )

    logger = logging.getLogger('extract_tables')
    logger.setLevel(logging.DEBUG)
    log_handler = logging.FileHandler(os.path.join(base_dir, 'logs', args.log))
    logger.addHandler(log_handler)

    extract_tables(
        output_path=args.output_dir,
        data_base_dir=args.data_base_dir,
        begin_date=args.begin_date,
        end_date=args.end_date,
        lang=args.lang,
        logger=logger,
    )
