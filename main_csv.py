from pyspark.sql import SparkSession
from yaml_reader import YamlReader


def init_spark_session():
    return SparkSession.builder.getOrCreate()


def get_config(file_path: str):
    yml_reader = YamlReader(file_path=file_path)
    return yml_reader.get_conf()


def read_data(spark_session):
    return spark_session.read.csv("oxford-government-response.csv", header=True)

# url = 'https://storage.googleapis.com/covid19-open-data/v3/oxford-government-response.csv'
# spark.sparkContext.addFile(url)
# spark.read.csv(SparkFiles.get("oxford-government-response.csv"), header=True))


spark = init_spark_session()
config = get_config(file_path='config.yml')
print("config: ", config)
df = read_data(spark_session=spark)
df.show()
df.printSchema()

