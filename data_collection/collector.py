from data_collection.api_wrapper.api_wrapper_factory import create_api_wrapper
from dateutil import parser
from data_collection.dataaccess.status import StatusSparkDA

class Collector:
    def __init__(self, api_version=1):
        self.status_da = StatusSparkDA()
        self.api = create_api_wrapper(api_version)

    def collect_status_by_keyword(self, keywords, from_time, to_time, pages=1, **kwargs):
        """

        :param pages: number of pages to return
        :param from_time: date time in string with format YYYY-mm-dd HH:MM:SS
        :param to_time: date time in string
        :param keywords: list of strings
        :param kwargs: other required keyword argument
        :return:
        """

        print(kwargs)
        until_dt = parser.parse(to_time)
        statuses = self.api.get_tweets_by_keywords(keywords=keywords, until=str(until_dt.date()), pages=pages, **kwargs)

        #TODO: store tweets using spark probably!
        self.status_da.save_statuses(statuses, '/home/navid/test.parquet').show()


collector = Collector()
