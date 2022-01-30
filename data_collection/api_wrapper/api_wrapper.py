from data_collection.config import credentials
from tweepy.models import User


class ApiWrapper:
    access_token = credentials.access_token
    access_token_secret = credentials.access_token_secret
    api_key = credentials.api_key
    api_secret = credentials.api_secret

    def __init__(self):
        pass

    def authenticate(self):
        pass

    def get_user_by_username(self, username: str) -> User:
        pass

    def get_user_by_id(self, user_id: int) -> User:
        pass
