from data_reader_writer.base_reader_writer import BaseReaderWriter
from data_reader_writer.local_file_reader_writer import LocalFileReaderWriter
from data_reader_writer.s3_file_reader_writer import S3FileReaderWriter
from data_reader_writer.bigquery_file_reader_writer import BigQueryFileReaderWriter
from pyspark.sql import SparkSession


def get_file_reader(config: dict, spark_session: SparkSession) -> BaseReaderWriter:
    if config.get('connection').get('file_system') == 'local':
        return LocalFileReaderWriter(spark_session=spark_session, config=config)
    elif config.get('connection').get('file_system') == 's3':
        return S3FileReaderWriter(spark_session=spark_session, config=config)
    elif config.get('connection').get('file_system') == 'gcp':
        return BigQueryFileReaderWriter(spark_session=spark_session, config=config)
