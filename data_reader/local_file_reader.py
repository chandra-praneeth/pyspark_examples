from data_reader.base_reader import BaseReader


class LocalFileReader(BaseReader):

    def read(self):
        file_paths: str = self.config.get('connection').get('file_paths')
        return self.spark_session.read.csv(file_paths, header=True)
