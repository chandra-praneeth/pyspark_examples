from pyspark.sql import DataFrame

from data_reader_writer.base_reader_writer import BaseReader


class LocalFileReader(BaseReader):

    def read(self):
        file_paths: str = self.config.get('connection').get('file_paths')
        return self.spark_session.read.csv(file_paths, header=True)

    def write(self, df: DataFrame):
        output_path: str = self.config.get('connection').get('output_dir_path')
        df\
            .write \
            .option("header", "true")\
            .option("sep", ",")\
            .mode("overwrite")\
            .csv(output_path)

