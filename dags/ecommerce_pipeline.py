from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import json
import random
from faker import Faker
import logging

default_args = {
    'owner': 'abdulrub',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    dag_id='ecommerce_sales_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    description='End-to-end e-commerce sales analytics pipeline',
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['ecommerce', 'analytics', 'etl']
)
def ecommerce_pipeline():
    
    @task()
    def generate_sales_data():
        """Generate fake e-commerce sales data"""
        fake = Faker()
        
        # Generate orders data
        orders = []
        for i in range(100):  # 100 orders per day
            order = {
                'order_id': f'ORD_{fake.uuid4()[:8]}',
                'customer_id': f'CUST_{random.randint(1000, 9999)}',
                'product_id': f'PROD_{random.randint(100, 999)}',
                'order_date': fake.date_between(start_date='-1d', end_date='today').isoformat(),
                'quantity': random.randint(1, 5),
                'unit_price': round(random.uniform(10.0, 500.0), 2),
                'total_amount': 0,  # Will calculate
                'status': random.choice(['completed', 'pending', 'cancelled']),
                'created_at': datetime.now().isoformat()
            }
            order['total_amount'] = round(order['quantity'] * order['unit_price'], 2)
            orders.append(order)
        
        # Generate customers data
        customers = []
        for i in range(50):  # 50 customers
            customer = {
                'customer_id': f'CUST_{1000 + i}',
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number(),
                'city': fake.city(),
                'country': fake.country(),
                'registration_date': fake.date_between(start_date='-2y', end_date='today').isoformat(),
                'created_at': datetime.now().isoformat()
            }
            customers.append(customer)
        
        # Generate products data
        products = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
        for i in range(30):  # 30 products
            product = {
                'product_id': f'PROD_{100 + i}',
                'product_name': fake.catch_phrase(),
                'category': random.choice(categories),
                'brand': fake.company(),
                'price': round(random.uniform(10.0, 500.0), 2),
                'stock_quantity': random.randint(0, 100),
                'created_at': datetime.now().isoformat()
            }
            products.append(product)
        
        return {
            'orders': orders,
            'customers': customers,
            'products': products
        }
    
    @task()
    def upload_to_s3(data):
        """Upload generated data to S3"""
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket_name = 'your-ecommerce-bucket'  # Replace with your bucket
        
        date_str = datetime.now().strftime('%Y/%m/%d')
        
        try:
            # Upload orders
            orders_key = f'raw/orders/{date_str}/orders.json'
            s3_hook.load_string(
                string_data=json.dumps(data['orders']),
                key=orders_key,
                bucket_name=bucket_name,
                replace=True
            )
            
            # Upload customers
            customers_key = f'raw/customers/{date_str}/customers.json'
            s3_hook.load_string(
                string_data=json.dumps(data['customers']),
                key=customers_key,
                bucket_name=bucket_name,
                replace=True
            )
            
            # Upload products
            products_key = f'raw/products/{date_str}/products.json'
            s3_hook.load_string(
                string_data=json.dumps(data['products']),
                key=products_key,
                bucket_name=bucket_name,
                replace=True
            )
            
            logging.info(f"Data uploaded to S3 bucket: {bucket_name}")
            return {
                'orders_key': orders_key,
                'customers_key': customers_key,
                'products_key': products_key
            }
            
        except Exception as e:
            logging.error(f"S3 upload failed: {str(e)}")
            raise
    
    @task()
    def load_to_snowflake(s3_keys):
        """Load data from S3 to Snowflake staging tables"""
        # This will be implemented with Snowflake hook
        logging.info("Loading data to Snowflake staging tables")
        # Placeholder for Snowflake integration
        return "Data loaded to Snowflake"
    
    @task()
    def trigger_dbt_run():
        """Trigger dbt transformations"""
        # This will run dbt models
        logging.info("Triggering dbt transformations")
        # Placeholder for dbt integration
        return "dbt transformations completed"
    
    @task()
    def data_quality_checks():
        """Run data quality validations"""
        logging.info("Running data quality checks")
        # Placeholder for data quality tests
        return "Data quality checks passed"
    
    # Define task dependencies
    data = generate_sales_data()
    s3_keys = upload_to_s3(data)
    snowflake_load = load_to_snowflake(s3_keys)
    dbt_run = trigger_dbt_run()
    quality_checks = data_quality_checks()
    
    data >> s3_keys >> snowflake_load >> dbt_run >> quality_checks

ecommerce_pipeline()