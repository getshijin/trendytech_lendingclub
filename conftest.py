import pytest
from lib.Utils import get_spark_session


@pytest.fixture
def spark():
    "create spark session"
    spark_session = get_spark_session('LOCAL')
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    'gives expceted result'
    result_schema = 'state string,count int'
    return spark.read.format('csv').schema(result_schema).option('header','true').load('data/testfile/Customer_agg.csv')