# Connector connects to S3 or GCP and tries to read data from cloud provider
# and read over spark
# Load configuration and connect to s3 or GCP
# Write unit tests too
# https://aws.amazon.com/blogs/big-data/a-public-data-lake-for-analysis-of-covid-19-data/
# https://dj2taa9i652rf.cloudfront.net/
# create a virtual env inside the directory
# Install libraries needed
# update readme document as we go
# Questions:
# 1. URL is publicly accessible, what is the need for s3/bigquery connector?
# 2. Any sample Bigquery table/s3 path which is publicly available for testing?

from pyspark.sql import SparkSession, DataFrame


class BaseReader:

    def __init__(self, spark_session, config: dict):
        self.spark_session: SparkSession = spark_session
        self.config = config

    def connect(self):
        pass

    def read(self):
        pass

    def write(self, df: DataFrame):
        pass
