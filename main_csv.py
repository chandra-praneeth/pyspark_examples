from pyspark.sql import SparkSession
from data_reader_writer.reader_writer_factory import get_file_reader
from data_reader_writer.base_reader_writer import BaseReaderWriter
from yaml_reader import YamlReader
import itertools


def init_spark_session():
    return SparkSession.builder.getOrCreate()


def get_config(file_path: str):
    yml_reader = YamlReader(file_path=file_path)
    return yml_reader.get_conf()


# Getting combinations: https://stackoverflow.com/a/464882/4987448
# https://docs.python.org/3/library/itertools.html#itertools.combinations
def get_grouping_sets(config: dict) -> str:
    columns: list = config.get('columns')
    no_of_dimensions: int = config.get('no_of_dimensions')
    grouping_sets: list = []
    # Calculate nCr where n = no of columns, r = no of dimensions
    for _r in range(no_of_dimensions+1):
        combinations: list = list(itertools.combinations(columns, _r))
        print("r: ", _r)
        print("combinations: ", combinations)
        combinations_modified: list = []
        for _combination in combinations:
            _comb: list = list(_combination)
            _comb.append("date")
            combinations_modified.append(
                "(" + ",".join(_comb) + ")"
            )

        print("combinations_modified: ", combinations_modified)
        grouping_sets.extend(combinations_modified)
    print("grouping_sets: ", grouping_sets)
    print("len(grouping_sets):", len(grouping_sets))
    _grouping_sets: str = ",".join(grouping_sets)
    print("_grouping_sets_joined", _grouping_sets)
    return _grouping_sets


def prepare_select_clause(config: dict):
    # Date column
    date_col: str = config.get('date_column')
    granularity: str = config.get('granularity')
    if granularity == 'weekly':
        date_col = f"TO_DATE(DATE_TRUNC('WEEK', {date_col})) AS date"
    print("date_col:", date_col)

    # Metrics aggregation
    aggregations: list[dict] = config.get('aggregations')
    _agg: list = list(map(lambda agg: agg.get('formula') + ' AS ' + agg.get('name'), aggregations))
    agg_cols: str = ','.join(_agg)
    print("agg_cols", agg_cols)

    # Dimensions concatenation
    # https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.concat.html
    concat_cols: list = []
    columns: list = config.get('columns', [])
    for _col in columns:
        _concat_col = f'CONCAT("{_col}=", {_col})'
        concat_cols.append(_concat_col)

    concat_cols_str: str = ",".join(concat_cols)
    # https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.concat_ws.html
    # Sample:
    # CONCAT_WS(',',
    #   CONCAT("location_key=", location_key),
    #   CONCAT("school_closing=", school_closing)
    # )

    concat_cols_str = f"CONCAT_WS(',',{concat_cols_str}) AS output"
    print("concat_cols_str: ", concat_cols_str)

    # Clause preparation
    select_clause: str = ",".join([date_col, concat_cols_str, agg_cols])
    print("select clause: ", select_clause)
    return select_clause


def main():
    spark = init_spark_session()
    config = get_config(file_path='config.yml')
    print("config: ", config)
    select_clause: str = prepare_select_clause(config)
    grouping_sets: str = get_grouping_sets(config)
    # exit(1)
    file_reader_writer: BaseReaderWriter = get_file_reader(config=config, spark_session=spark)
    source_df = file_reader_writer.read()
    source_df.show()
    source_df.printSchema()

    source_df.createOrReplaceTempView("source")

    stage_sql_query: str = f"""
    SELECT 
        {select_clause}
    FROM source
    GROUP BY GROUPING SETS({grouping_sets})
    ORDER BY date, output
    """
    print("stage_sql_query:", stage_sql_query)

    stage_df = spark.sql(stage_sql_query)

    stage_df.show(n=100, truncate=False)
    file_reader_writer.write(stage_df)


main()
