from data_collection.api_wrapper.api_wrapper import ApiWrapper
from tweepy.models import User

import tweepy


class V1ApiWrapper(ApiWrapper):
    def __init__(self):
        ApiWrapper.__init__(self)
        self.api = None

    def authenticate(self):
        auth = tweepy.OAuthHandler(self.com_key, self.com_secret)
        auth.set_access_token(self.acc_key, self.acc_secret)
        self.api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

    def get_user_by_username(self, username: str) -> User:
        return self.api.get_user(screen_name=username)

    def get_user_by_id(self, user_id: int) -> User:
        return self.api.get_user(user_id)
