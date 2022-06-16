from data_collection.util.spark import get_spark
import pyspark.sql.functions as F
import argparse
import os

RAW_DATA_PATH = '/data/navid/processed_tweets'

def create_retweet_graph(output_path, all_tweets_path):
    spark = get_spark(cores=10, driver_mem=12)

    all_tweets_df = spark.read.parquet(os.path.join(all_tweets_path, '*', '*'))

    #TODO: modify the function to filter any time interval
    # filtering tweets before election time
    filtered_tweets = (
        all_tweets_df
        .filter(F.to_date(F.from_unixtime('created_at')) < '2020-11-10')
    )

    tweet_user_df = (
        filtered_tweets
        #TODO: modify the function to filter only tweets that contain specific keyowrds
        #.filter(F.lower(F.col('text')).contains('biden'))
        .select(F.col('id').alias('rt_id'), F.col('uid').alias('rt_uid'))
    )

    retweet_df = (
        filtered_tweets
        .select('uid', 'rt_id')
        .filter(F.col('rt_id').isNotNull())
    )

    graph_df = (
        retweet_df
        .join(tweet_user_df, on='rt_id', how='left')
        .groupBy('uid', 'rt_uid').agg(F.count(F.lit(1)).alias('rt_times'))
    )

    graph_df.write.parquet(output_path, mode='overwrite')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir', help="The path to store the retweet network parquet file", required=True)
    args = parser.parse_args()

    create_retweet_graph(output_path=args.output_dir, all_tweets_path=RAW_DATA_PATH)