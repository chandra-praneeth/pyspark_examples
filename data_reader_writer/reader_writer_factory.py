from data_reader_writer.base_reader_writer import BaseReader
from data_reader_writer.local_file_reader_writer import LocalFileReader
from data_reader_writer.s3_file_reader_writer import S3FileReader
from data_reader_writer.bigquery_file_reader_writer import BigQueryFileReader
from pyspark.sql import SparkSession


def get_file_reader(config: dict, spark_session: SparkSession) -> BaseReader:
    if config.get('connection').get('file_system') == 'local':
        return LocalFileReader(spark_session=spark_session, config=config)
    elif config.get('connection').get('file_system') == 's3':
        return S3FileReader(spark_session=spark_session, config=config)
    elif config.get('connection').get('file_system') == 'gcp':
        return BigQueryFileReader(spark_session=spark_session, config=config)
