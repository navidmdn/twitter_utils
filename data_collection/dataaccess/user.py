from typing import Dict


class User:
    def __init__(self, user_dict: Dict):
        self.uid = user_dict['id']
        self.screen_name = user_dict['screen_name']
        self.description = user_dict['description']
        self.verified = user_dict['verified']
        self.followers_count = user_dict['followers_count']
        self.friends_count = user_dict['friends_count']

