import itertools
import logging

logging.getLogger().setLevel(logging.INFO)


def get_date_column(date_col: str, granularity: str) -> str:
    if granularity == 'weekly':
        return f"TO_DATE(DATE_TRUNC('WEEK', {date_col})) AS date"
    return date_col


def get_aggregated_metrics(aggregations: list[dict]) -> str:
    _agg: list = list(
        map(
            lambda agg: agg.get('formula') + ' AS ' + agg.get('name'),
            aggregations
        )
    )
    agg_cols: str = ','.join(_agg)
    return agg_cols


# https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.concat.html
# https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.concat_ws.html
def get_concatenated_column_output(columns: list[str]) -> str:
    concat_cols: list = []
    for _col in columns:
        _concat_col = f'CONCAT("{_col}=", {_col})'
        concat_cols.append(_concat_col)
    concat_cols_str: str = ",".join(concat_cols)

    # Sample:
    # CONCAT('[',
    #     CONCAT_WS(',',
    #         CONCAT("location_key=", location_key),
    #         CONCAT("school_closing=", school_closing),
    #         CONCAT("cancel_public_events=", cancel_public_events)
    #     ),
    # ']') AS output
    concat_cols_str = f"CONCAT('[', CONCAT_WS(',',{concat_cols_str}), ']') AS output"
    return concat_cols_str


# Getting combinations: https://stackoverflow.com/a/464882/4987448
# https://docs.python.org/3/library/itertools.html#itertools.combinations
def get_grouping_sets(columns: list, no_of_dimensions: int ) -> str:
    grouping_sets: list = []
    # Calculate nCr where n = no of columns, r = no of dimensions
    for _r in range(no_of_dimensions+1):
        combinations: list = list(itertools.combinations(columns, _r))
        logging.info("r: " + str(_r))
        logging.info("combinations: " + str(combinations))
        combinations_modified: list = []
        for _combination in combinations:
            _comb: list = list(_combination)
            _comb.append("date")
            combinations_modified.append(
                "(" + ",".join(_comb) + ")"
            )

        logging.info("combinations_modified: " + str(combinations_modified))
        grouping_sets.extend(combinations_modified)
    logging.info("grouping_sets: " + str(grouping_sets))
    logging.info("len(grouping_sets): " + str(len(grouping_sets)))
    _grouping_sets: str = ",".join(grouping_sets)
    logging.info("_grouping_sets_joined: " + _grouping_sets)
    return _grouping_sets


