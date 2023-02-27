from pyspark.sql import SparkSession
from data_reader.reader_factory import get_file_reader
from data_reader.base_reader import BaseReader
from yaml_reader import YamlReader


def init_spark_session():
    return SparkSession.builder.getOrCreate()


def get_config(file_path: str):
    yml_reader = YamlReader(file_path=file_path)
    return yml_reader.get_conf()


def read_data(config: dict, spark_session: SparkSession):
    file_reader: BaseReader = get_file_reader(config=config, spark_session=spark_session)
    return file_reader.read()
    # return spark_session.read.csv("oxford-government-response.csv", header=True)


def prepare_select_clause():
    pass


def run_daily_query(spark: SparkSession):
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


def run_weekly_query(spark: SparkSession):
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


def main():
    spark = init_spark_session()
    config = get_config(file_path='config.yml')
    print("config: ", config)
    source_df = read_data(config=config, spark_session=spark)
    source_df.show()
    source_df.printSchema()

    source_df.createOrReplaceTempView("source")

    stage_df = spark.sql(
        """
        SELECT 
            -- date,
            TO_DATE(DATE_TRUNC('WEEK', date)) AS week_start_date,
            -- DAY(date),
            -- DAYOFMONTH(date),
            -- DAYOFWEEK(date),
            concat_ws(',',
                concat("location_key=", location_key),
                concat("cancel_public_events=", cancel_public_events),
                concat("restrictions_on_gatherings=", restrictions_on_gatherings)
            ) as output,
            sum(income_support)
        FROM source
        GROUP BY ROLLUP(week_start_date, location_key, cancel_public_events, restrictions_on_gatherings)
        ORDER BY week_start_date, output
        """
    )

    stage_df.show(truncate=False)

    # run_daily_query()
    # run_weekly_query()

main()
