from pyspark.sql import SparkSession
from yaml_reader import YamlReader


def init_spark_session():
    return SparkSession.builder.getOrCreate()


def get_config(file_path: str):
    yml_reader = YamlReader(file_path=file_path)
    return yml_reader.get_conf()


def read_data(spark_session):
    return spark_session.read.csv("oxford-government-response.csv", header=True)

def prepare_select_clause():
    pass


def run_daily_query():
    result_df_daily = spark.sql(
        """
        select 
            date,
            location_key, 
            school_closing, 
            cancel_public_events,
            sum(income_support) as total_income_support,
            sum(international_support) as total_international_support,
            sum(fiscal_measures) as total_fiscal_measures
        from source
        group by cube(date, location_key, school_closing, cancel_public_events)
        order by location_key, school_closing, cancel_public_events
        """
    )
    result_df_daily.show()
    result_df_daily.printSchema()


def run_weekly_query():
    result_df_weekly = spark.sql(
        """
        select 
            date_trunc(date, 'week') as week,
            location_key, 
            school_closing, 
            cancel_public_events,
            sum(income_support) as total_income_support,
            sum(international_support) as total_international_support,
            sum(fiscal_measures) as total_fiscal_measures
        from source
        group by cube(week, location_key, school_closing, cancel_public_events)
        order by week, location_key, school_closing, cancel_public_events
        """
    )
    result_df_weekly.show()
    result_df_weekly.printSchema()

spark = init_spark_session()
config = get_config(file_path='config.yml')
print("config: ", config)
df = read_data(spark_session=spark)
df.show()
df.printSchema()

df.createOrReplaceTempView("source")

spark.sql(
    """
    SELECT 
        date, 
        TO_DATE(DATE_TRUNC('WEEK', date)) AS week_start_date,
        DAY(date),
        DAYOFMONTH(date),
        DAYOFWEEK(date)
    FROM source
    """
).show()
# run_daily_query()
# run_weekly_query()
