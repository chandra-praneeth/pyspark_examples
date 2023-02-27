from data_reader.base_reader import BaseReader
from data_reader.local_file_reader import LocalFileReader
from data_reader.s3_file_reader import S3FileReader
from data_reader.bigquery_file_reader import BigQueryFileReader
from pyspark.sql import SparkSession


def get_file_reader(config: dict, spark_session: SparkSession) -> BaseReader:
    if config.get('connection').get('file_system') == 'local':
        return LocalFileReader(spark_session=spark_session, config=config)
    elif config.get('connection').get('file_system') == 's3':
        return S3FileReader(spark_session=spark_session, config=config)
    elif config.get('connection').get('file_system') == 'gcp':
        return BigQueryFileReader(spark_session=spark_session, config=config)
