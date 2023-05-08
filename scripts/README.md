# Extracting gz files into parquet using pyspark


## basic usage

You can use the `extract_tables_from_dh.py` file to extract both tweets and users object in a relational fashion
into parquet files for faster and lazy loading. To do so, run the file using the following command:

```
python extract_tables_from_dh.py \
  --data_base_dir "path to the .gz file base path containing folders for each date"
  --output_dir "the base path to the extracted parquet files"
  --begin_date "2022-01-01"
  --end_date "2022-12-01"
  --log "path to a log file"
  --lang "language you want to filter tweets based on. It is based on the language field of a tweet object."
```

if you don't want to filter any languages don't specify the lang argument.

## advanced usage

### changing the schema of Tweet and User objects

The Tweet and User classes are defined in `data_collection/dataaccess/` directory. You can add, delete or modify the 
way each attribute is used over there.

### memory management

All spark related configs such as number of threads and memory it use are defined in `data_collection.util.spark` you
can change those configs according to the system requirements.

There is also support for reading from .gz file in batches in order to avoid out of memory error. You can manage the
batch size in `extract_tables_from_dh.py` script manually inside `process_tweets_and_users` method.
