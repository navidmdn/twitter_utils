from data_collection.util.spark import get_spark
from typing import List
from tweepy.models import Status


class StatusSparkDA:
    def __init__(self):
        self.spark = get_spark()

    schema = [
        'id',
        'created_at'
        'user_id',
        'text',
    ]

    def save_statuses(self, statuses: List[Status], path):

        data = []
        for status in statuses:
            data.append(
                (status.id, status.created_at, status.user.id, status.text)
            )
        df = self.spark.createDataFrame(data, schema=self.schema)
        return df
        #self.spark.write.parquet(path, mode='overwrite')
