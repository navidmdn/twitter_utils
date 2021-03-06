from typing import Dict


class User:
    def __init__(self, user_dict: Dict):
        self.uid = user_dict['id']
        self.created_at = user_dict['created_at']
        self.screen_name = user_dict['screen_name']
        self.description = user_dict['description']
        self.verified = user_dict['verified']
        self.followers_count = user_dict['followers_count']
        self.friends_count = user_dict['friends_count']
        self.record_time = None

    def set_record_time(self, dt):
        self.record_time = dt

