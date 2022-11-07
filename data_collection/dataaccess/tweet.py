

class Tweet:
    def __init__(self, tweet_dict):
        self.id = tweet_dict['id']
        self.created_at = tweet_dict['created_at']

        is_retweet = 'retweeted_status' in tweet_dict
        is_quoted = 'quoted_status' in tweet_dict

        if is_retweet:
            self.retweeted_id = tweet_dict['retweeted_status']['id']
            self.retweeted_uid = tweet_dict['retweeted_status']['user']['id']
            self.retweeted_screen_name = tweet_dict['retweeted_status']['user']['screen_name']
        else:
            self.retweeted_uid = self.retweeted_id = None

        if is_quoted:
            self.quoted_id = tweet_dict['quoted_status']['id']
            self.quoted_uid = tweet_dict['quoted_status']['user']['id']
            self.quoted_screen_name = tweet_dict['quoted_status']['user']['screen_name']
        else:
            self.quoted_uid = self.quoted_id = None

        if 'extended_tweet' in tweet_dict:
            self.text = tweet_dict['extended_tweet']['full_text']
        else:
            self.text = tweet_dict['text']

        if is_retweet:
            self.text = None

        self.in_reply_to_status_id = tweet_dict['in_reply_to_status_id']
        self.in_reply_to_user_id = tweet_dict['in_reply_to_user_id']
        self.in_reply_to_screen_name = tweet_dict['in_reply_to_screen_name']

        self.user_id = tweet_dict['user']['id']
        self.lang = tweet_dict['lang']

        hashtags = tweet_dict['entities']['hashtags']
        self.hashtags = [h['text'] for h in hashtags]

        mentions = tweet_dict['entities']['user_mentions']
        self.mention_ids = [m['id'] for m in mentions]
        self.mention_screen_names = [m['screen_name'] for m in mentions]

        urls = tweet_dict['entities']['urls']
        self.urls = [u['expanded_url'] for u in urls]

        if 'extended_entities' in tweet_dict:
            media = tweet_dict['extended_entities']['media'] if 'media' in tweet_dict['extended_entities'] else []
            self.media = [m['expanded_url'] for m in media]
        else:
            media = tweet_dict['entities']['media'] if 'media' in tweet_dict['entities'] else []
            self.media = [m['expanded_url'] for m in media]



