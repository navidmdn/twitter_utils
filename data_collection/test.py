from data_collection.collector import collector


collector.collect_by_keyword(
    keywords=['iran'],
    from_time='2021-09-25',
    to_time='2021-09-26',
    retweet=False,
    count=10,
    pages=3
)