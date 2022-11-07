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
        self.name = user_dict['name']

        self.location = self.safe_get('location', user_dict)
        self.url = self.safe_get('url', user_dict)
        self.protected = self.safe_get('protected', user_dict)
        self.listed_count = self.safe_get('listed_count', user_dict)
        self.favorites_count = self.safe_get('favorites_count', user_dict)
        self.statuses_count = self.safe_get('statuses_count', user_dict)
        self.withheld_in_countries = self.safe_get('withheld_in_countries', user_dict)
        self.withheld_scope = self.safe_get('withheld_scope', user_dict)

    def set_record_time(self, dt):
        self.record_time = dt

    @staticmethod
    def safe_get(attr: str, d: Dict):
        if attr in d:
            return d[attr]
        return None