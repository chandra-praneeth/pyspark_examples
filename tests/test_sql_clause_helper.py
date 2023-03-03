from src.sql_clause_helper import get_date_column, get_aggregated_metrics, \
    get_concatenated_column_output, get_grouping_sets


def test_date_column():
    assert get_date_column('date', 'daily') == 'date'
    assert get_date_column('timestamp', 'daily') == 'timestamp'

    assert get_date_column('date', 'weekly') == "TO_DATE(DATE_TRUNC('WEEK', date)) AS date"
    assert get_date_column('timestamp', 'weekly') == "TO_DATE(DATE_TRUNC('WEEK', timestamp)) AS date"


def test_aggregated_metrics():
    aggregations: list = [
        {'name': 'total_income_support', 'formula': 'sum(income_support)'}
    ]
    assert get_aggregated_metrics(aggregations) == "sum(income_support) AS total_income_support"

    aggregations: list = [
        {'name': 'total_income_support', 'formula': 'sum(income_support)'},
        {'name': 'total_international_support', 'formula': 'sum(international_support)'}
    ]

    expected_output: str = 'sum(income_support) AS total_income_support,sum(international_support) AS total_international_support'
    assert get_aggregated_metrics(aggregations) == expected_output


def test_concatenated_column_output():
    columns: list[str] = ['location_key']
    expected_output: str = """CONCAT('[', CONCAT_WS(',',CONCAT("location_key=", location_key)), ']') AS output"""
    assert get_concatenated_column_output(columns) == expected_output

    columns: list[str] = ['location_key', 'school_closing']
    expected_output: str = """CONCAT('[', CONCAT_WS(',',CONCAT("location_key=", location_key),CONCAT("school_closing=", school_closing)), ']') AS output"""
    assert get_concatenated_column_output(columns) == expected_output


def test_grouped_sets_clause():
    # 1 column
    columns: list[str] = ['a']
    no_of_dimensions: int = 0
    assert get_grouping_sets(columns=columns, no_of_dimensions=no_of_dimensions) == "(date)"

    no_of_dimensions: int = 1
    assert get_grouping_sets(columns=columns, no_of_dimensions=no_of_dimensions) == "(date),(a,date)"

    no_of_dimensions: int = 2
    assert get_grouping_sets(columns=columns, no_of_dimensions=no_of_dimensions) == "(date),(a,date)"

    # 2 columns
    columns: list[str] = ['a', 'b']
    no_of_dimensions: int = 0
    assert get_grouping_sets(columns=columns, no_of_dimensions=no_of_dimensions) == "(date)"

    no_of_dimensions: int = 1
    assert get_grouping_sets(
        columns=columns, no_of_dimensions=no_of_dimensions
    ) == "(date),(a,date),(b,date)"

    no_of_dimensions: int = 2
    assert get_grouping_sets(
        columns=columns,
        no_of_dimensions=no_of_dimensions
    ) == "(date),(a,date),(b,date),(a,b,date)"

    no_of_dimensions: int = 3
    assert get_grouping_sets(
        columns=columns,
        no_of_dimensions=no_of_dimensions
    ) == "(date),(a,date),(b,date),(a,b,date)"

    # 3 columns
    columns: list[str] = ['a', 'b', 'c']
    no_of_dimensions: int = 1
    assert get_grouping_sets(
        columns=columns,
        no_of_dimensions=no_of_dimensions
    ) == "(date),(a,date),(b,date),(c,date)"

    no_of_dimensions: int = 2
    assert get_grouping_sets(
        columns=columns,
        no_of_dimensions=no_of_dimensions
    ) == "(date),(a,date),(b,date),(c,date),(a,b,date),(a,c,date),(b,c,date)"

    no_of_dimensions: int = 3
    assert get_grouping_sets(
        columns=columns,
        no_of_dimensions=no_of_dimensions
    ) == "(date),(a,date),(b,date),(c,date),(a,b,date),(a,c,date),(b,c,date),(a,b,c,date)"
