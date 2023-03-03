import logging

from pyspark.sql import SparkSession

from data_reader_writer.base_reader_writer import BaseReaderWriter
from data_reader_writer.reader_writer_factory import get_file_reader
from sql_clause_helper import get_date_column, get_aggregated_metrics, \
    get_concatenated_column_output, get_grouping_sets
from yaml_reader import YamlReader

logging.getLogger().setLevel(logging.INFO)


def init_spark_session():
    return SparkSession.builder.getOrCreate()


def get_config(file_path: str):
    yml_reader = YamlReader(file_path=file_path)
    return yml_reader.get_conf()


def prepare_select_clause(config: dict):
    # Date column
    date_col: str = get_date_column(
        date_col=config.get('date_column'),
        granularity=config.get('granularity')
    )
    logging.info("date_col: " + date_col)

    # Metrics aggregation
    agg_cols: str = get_aggregated_metrics(
        config.get('aggregations')
    )
    logging.info("agg_cols: " + agg_cols)

    # Dimensions concatenation
    concat_cols_str = get_concatenated_column_output(config.get('columns'))
    logging.info("concat_cols_str: " + concat_cols_str)

    # Clause preparation
    select_clause: str = ",".join([date_col, concat_cols_str, agg_cols])
    logging.info("select clause: " + select_clause)
    return select_clause


def main():
    # Spark init and read config
    spark = init_spark_session()
    config = get_config(file_path='src/config.yml')
    logging.info("config: " + str(config))

    # Prepare SparkSQL statements
    select_clause: str = prepare_select_clause(config)
    grouping_sets: str = get_grouping_sets(
        config.get('columns'),
        config.get('no_of_dimensions')
    )
    # exit(1)

    # Read input data and create a sql view
    file_reader_writer: BaseReaderWriter = get_file_reader(config=config, spark_session=spark)
    source_df = file_reader_writer.read()
    source_df.show()
    source_df.printSchema()
    source_df.createOrReplaceTempView("source")

    # SparkSQL transformation query
    stage_sql_query: str = f"""
    SELECT
        {select_clause}
    FROM source
    GROUP BY GROUPING SETS({grouping_sets})
    ORDER BY date, output
    """
    # Sample Query
    # stage_sql_query: str = """
    # SELECT
    #     TO_DATE(DATE_TRUNC('WEEK', date)) AS date,
    #     location_key,
    #     school_closing,
    #     CONCAT("[",
    #         CONCAT_WS(',',
    #             CONCAT("location_key=", location_key),
    #             CONCAT("school_closing=", school_closing)
    #         ),
    #     "]") AS output,
    #     sum(income_support) AS total_income_support
    # FROM source
    # WHERE date = '2020-01-01'
    # GROUP BY GROUPING SETS( (date), (location_key,date), (school_closing,date), (location_key,school_closing,date))
    # ORDER BY date, output
    # """
    logging.info("stage_sql_query: " + stage_sql_query)

    stage_df = spark.sql(stage_sql_query)
    stage_df.show(truncate=False)

    # Write to output
    file_reader_writer.write(stage_df)


main()
