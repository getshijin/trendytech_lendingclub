from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders
from lib.ConfigReader import get_app_config



def test_read_customer_df(spark):
    #spark = get_spark_session('LOCAL')
    customers_count = read_customers(spark,'LOCAL').count()
    assert customers_count == 12435

def test_read_orders_df(spark):
    #spark = get_spark_session('LOCAL')
    corders_count = read_orders(spark,'LOCAL').count()
    assert corders_count == 68881 

def test_filter_closed_orders_df(spark):
    #spark = get_spark_session('LOCAL')
    order_df = read_orders(spark,'LOCAL')
    filter_closed_orders_count = filter_closed_orders(order_df).count()
    assert filter_closed_orders_count == 7556 

def test_read_app_config(spark):
    config = get_app_config('LOCAL')
    assert config['orders.file.path'] == 'data/orders_wh.csv' and  config['customers.file.path'] == 'data/customers.csv'

