import argparse
import os
import json
import gzip
import io
import glob
import pandas as pd

from datetime import datetime
from dateutil import parser
from datetime import timedelta


# base_files_path_template = "/data/dnc2020/raw_tweets/{date}*/*.gz"
# error_file_path = 'errors.log'
# output_path_template = '/data/navid/processed_tweets/processed_tweets_{date}/'
# output_user_path_template = '/data/navid/processed_users/processed_users_{date}/'
# begin_date = '2020-01-01'
# end_date = '2020-12-31'
# batch_size = 100000


def log_error(e, msg, path='errors.log'):
	with open(path, "a") as f:
		f.write(f"\n{datetime.now()}: {msg} caused by: {e}\n")


def read_gz_file_content(path, batch_size):
	print(f'loading gz file {path}')

	tweets = {}
	idx = 1

	gz = gzip.open(path, 'r')
	f = io.BufferedReader(gz)
	try:
		for line in f:
			content = json.loads(line)
			tweet_id = content["id"]
			tweets.update({tweet_id: content})
			idx += 1

			if idx > batch_size:
				yield tweets, False
				idx = 1
				del tweets
				tweets = {}

		gz.close()
		yield tweets, True

	except EOFError as error:
		log_error(error, 'eof error')
		raise error
	except Exception as e:
		log_error(e, 'failed to process gz file')
		raise e


def build_tweet_df(tweet_raw_dict):

	tweet_ids = []
	tweet_texts = []
	tweet_user_ids = []
	tweet_user_usernames = []
	retweet_id = []
	reply_id = []
	quote_id = []
	mentions = []
	timestamps = []

	for tid, tweet_json in tweet_raw_dict.items():

		if 'extended_tweet' in tweet_json:
			text = tweet_json['extended_tweet']['full_text']
		elif 'retweeted_status' in tweet_json:
			if 'extended_tweet' in tweet_json['retweeted_status']:
				text = tweet_json['retweeted_status']['extended_tweet']['full_text']
			else:
				text = tweet_json['retweeted_status']['text']
		else:
			text = tweet_json['text']

		if len(text):
			tweet_texts.append(text)
		else:
			continue

		tweet_ids.append(str(tid))
		tweet_user_ids.append(tweet_json['user']['id'])
		tweet_user_usernames.append(tweet_json['user']['screen_name'])
		mentions.append([user['screen_name'] for user in tweet_json['entities']['user_mentions']])
		retweet_id.append(tweet_json['retweeted_status']['id_str'] if 'retweeted_status' in tweet_json else None)

		reply_id.append(tweet_json['in_reply_to_status_id_str'] if 'in_reply_to_status_id_str' in tweet_json else None)
		quote_id.append(tweet_json['quoted_status_id_str'] if 'quoted_status_id_str' in tweet_json else None)

		timestamps.append(int(parser.parse(tweet_json['created_at']).timestamp()))

	tweets_df = pd.DataFrame({
		'id': tweet_ids,
		'created_at': timestamps,
		'uid': tweet_user_ids,
		'username': tweet_user_usernames,
		'text': tweet_texts,
		'rt_id': retweet_id,
		'quote_id': quote_id,
		'reply_id': reply_id,
		'mentions': mentions,
	})

	return tweets_df


def build_user_df(tweet_raw_dict):

	user_ids = []
	user_descriptions = []
	usernames = []
	timestamps = []

	for tid, tweet_json in tweet_raw_dict.items():
		if 'user' in tweet_json:
			usr_json = tweet_json['user']
		else:
			continue

		timestamps.append(int(parser.parse(tweet_json['created_at']).timestamp()))
		user_ids.append(usr_json['id'])
		user_descriptions.append(usr_json['description'])
		usernames.append(usr_json['screen_name'])

	tweets_df = pd.DataFrame({
		'id': user_ids,
		'created_at': timestamps,
		'username': usernames,
		'description': user_descriptions,
	})

	return tweets_df


def preprocess_and_save(search_path, output_path_base, batch_size=100000, mode='tweet'):

	for path in glob.glob(search_path):
		done = False
		batch_no = 1
		try:
			for content_dict, done in read_gz_file_content(path, batch_size):
				if done:
					break
				print(f'finished batch: {batch_no}')
				if mode == 'tweet':
					result_df = build_tweet_df(content_dict)
				elif mode == 'user':
					result_df = build_user_df(content_dict)
				else:
					raise NotImplementedError("this mode is not implemented yet!")
				result_df.to_parquet(
					os.path.join(output_path_base, f"{int(datetime.now().timestamp())}.parquet")
				)
				batch_no += 1
		except Exception as e:
			log_error(e, 'unexpected error occurred!')


def run(args):
	current_dt = parser.parse(args.begin_date)

	while current_dt != parser.parse(args.end_date):
		current_date = current_dt.date()
		output_path = args.output_path_template.format(date=current_date)
		os.makedirs(output_path, exist_ok=True)
		print(f'processing {current_date}')
		search_path = args.stream_path_template.format(date=current_date)

		preprocess_and_save(search_path, output_path, batch_size=args.batch_size, mode=args.mode)
		current_dt = current_dt + timedelta(days=1)


if __name__ == '__main__':
	arg_parser = argparse.ArgumentParser()
	arg_parser.add_argument("--stream_path_template", default="/data/dnc2020/raw_tweets/{date}*/*.gz", )
	arg_parser.add_argument("--output_path_template", required=True)
	arg_parser.add_argument("--begin_date", required=True)
	arg_parser.add_argument("--end_date", required=True)
	arg_parser.add_argument("--batch_size", default=100000, type=int)
	arg_parser.add_argument("--mode", default='tweet')
	args = arg_parser.parse_args()

	run(args)
