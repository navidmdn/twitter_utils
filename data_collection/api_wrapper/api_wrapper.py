from data_collection.config import credentials
from tweepy.models import User


class ApiWrapper:
    acc_key = credentials.acc_key
    acc_secret = credentials.acc_secret
    com_key = credentials.com_key
    com_secret = credentials.com_secret

    def __init__(self):
        pass

    def authenticate(self):
        pass

    def get_user_by_username(self, username: str) -> User:
        pass

    def get_user_by_id(self, user_id: int) -> User:
        pass
