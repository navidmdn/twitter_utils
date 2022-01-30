from data_collection.api_wrapper.api_wrapper import ApiWrapper
from tweepy.models import User, Status
from typing import List
from math import inf
import tweepy


class V1ApiWrapper(ApiWrapper):
    def __init__(self):
        ApiWrapper.__init__(self)
        self.api = None

    def authenticate(self):
        auth = tweepy.OAuthHandler(self.api_key, self.api_secret)
        auth.set_access_token(self.access_token, self.access_token_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True)

    def get_user_by_username(self, username: str) -> User:
        return self.api.get_user(screen_name=username)

    def get_user_by_id(self, user_id: int) -> User:
        return self.api.get_user(user_id)

    def get_tweets_by_keywords(self, keywords, retweet=None, pages=1, **kwargs) -> List[Status]:
        """

        :param pages: number of pages
        :param keywords: a list of keywords which we are looking for
        :param retweet: can be None which doesn't filter at all, True which keeps retweets and False which keeps non-
                        retweets
        :return:
        """
        statuses = []
        keywords_q = " OR ".join(keywords)
        query = f"({keywords_q} "

        if retweet is not None:
            if not retweet:
                query += "-"
            query += "filter:retweets"

        cursor = tweepy.Cursor(self.api.search_tweets, query, **kwargs).pages(pages)

        for search_result in cursor:
            for status in search_result:
                statuses.append(status)

        return statuses
