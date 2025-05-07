import pytest
from lib.DataReader import read_customers,read_orders
from lib.DataManipulation import filter_closed_orders,count_orders_state, filter_orders_generic
from lib.ConfigReader import get_app_config


@pytest.mark.skip
def test_read_customer_df(spark):
    #spark = get_spark_session('LOCAL')
    customers_count = read_customers(spark,'LOCAL').count()
    assert customers_count == 12435

@pytest.mark.skip
def test_read_orders_df(spark):
    #spark = get_spark_session('LOCAL')
    corders_count = read_orders(spark,'LOCAL').count()
    assert corders_count == 68881 

@pytest.mark.skip
def test_filter_closed_orders_df(spark):
    #spark = get_spark_session('LOCAL')
    order_df = read_orders(spark,'LOCAL')
    filter_closed_orders_count = filter_closed_orders(order_df).count()
    assert filter_closed_orders_count == 7556
    
@pytest.mark.skip
def test_read_app_config(spark):
    config = get_app_config('LOCAL')
    assert config['orders.file.path'] == 'data/orders_wh.csv' and  config['customers.file.path'] == 'data/customers.csv'

@pytest.mark.skip
def test_count_orders_state(spark,expected_results):
    #spark = get_spark_session('LOCAL')
    customers_df = read_customers(spark,'LOCAL')
    actuall_result = count_orders_state(customers_df)
    assert actuall_result.collect() == expected_results.collect()

@pytest.mark.skip
def test_check_closed_count(spark):
    order_df = read_orders(spark,'LOCAL')
    filter_closed_orders_count = filter_orders_generic(order_df,'CLOSED').count()
    assert filter_closed_orders_count == 7556

@pytest.mark.skip
def test_check_PENDING_PAYMENT_count(spark):
    order_df = read_orders(spark,'LOCAL')
    filter_closed_orders_count = filter_orders_generic(order_df,'PENDING_PAYMENT').count()
    assert filter_closed_orders_count == 15030

@pytest.mark.skip
def test_check_COMPLETE_count(spark):
    order_df = read_orders(spark,'LOCAL')
    filter_closed_orders_count = filter_orders_generic(order_df,'COMPLETE').count()
    assert filter_closed_orders_count == 22899



@pytest.mark.parametrize(
    "status, count",
    [('CLOSED', 7556), ('PENDING_PAYMENT',  15030), ('COMPLETE', 22899)]
)

def test_check_COMPLETE_count(spark,status,count):
    order_df = read_orders(spark,'LOCAL')
    filter_closed_orders_count = filter_orders_generic(order_df,status).count()
    assert filter_closed_orders_count == count