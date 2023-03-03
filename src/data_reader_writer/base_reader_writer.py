from pyspark.sql import SparkSession, DataFrame


class BaseReaderWriter:

    def __init__(self, spark_session, config: dict):
        self.spark_session: SparkSession = spark_session
        self.config = config

    def connect(self):
        pass

    def read(self):
        pass

    def write(self, df: DataFrame):
        pass
