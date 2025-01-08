"""
DAG для построения аналитических витрин
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


MYSQL_JDBC_URL = "jdbc:mysql://mysql:3306/orders"
MYSQL_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "com.mysql.cj.jdbc.Driver"
}

spark = SparkSession.builder \
    .appName('MySQL_to_Analytics_Marts') \
    .config('spark.jars', "/opt/airflow/jars/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()


def build_product_sales_mart():
    """
    Функция для создания витрины продаж по продуктам
    """
    order_details_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='order_details', properties=MYSQL_PROPERTIES)
    products_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='products', properties=MYSQL_PROPERTIES)
    categories_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='product_categories', properties=MYSQL_PROPERTIES)
    categories_df = categories_df.withColumnRenamed('category_id', 'id').withColumnRenamed('name', 'category_name')

    joined_df = order_details_df.join(products_df, 'product_id').join(
        categories_df, products_df['category_id'] == categories_df['id'], 'left'
    )
    result = joined_df.groupBy('product_id', 'name', 'description', 'category_id', 'category_name', 'price').agg(
        F.sum('total_price').alias('total_sales'),
        F.count('order_id').alias('total_orders'),
        F.avg('total_price').alias('avg_order_value'),
        F.max('total_price').alias('max_order_value')
    )
    result.write.jdbc(url=MYSQL_JDBC_URL, table='product_sales_mart', mode="overwrite", properties=MYSQL_PROPERTIES)


def build_user_orders_mart():
    """
    Функция для создания витрины заказов пользователей
    """
    orders_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='orders', properties=MYSQL_PROPERTIES)
    users_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='users', properties=MYSQL_PROPERTIES)
    orderdetails_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='order_details', properties=MYSQL_PROPERTIES)
    products_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='products', properties=MYSQL_PROPERTIES)
    categories_df = spark.read.jdbc(url=MYSQL_JDBC_URL, table='product_categories', properties=MYSQL_PROPERTIES)
    categories_df = categories_df.withColumnRenamed('name', 'category_name')

    joined_df = orders_df.join(users_df, 'user_id').join(orderdetails_df, 'order_id') \
        .join(products_df, 'product_id') \
        .join(categories_df, products_df['category_id'] == categories_df['category_id'], 'left')

    result = joined_df.groupBy('user_id', 'first_name', 'last_name', 'email', 'phone').agg(
        F.countDistinct('order_id').alias('total_orders'),
        F.sum('total_amount').alias('total_spent'),
        F.first('category_name').alias('favorite_category'),
        F.count('product_id').alias('most_purchased_product_count'),
        F.sum('total_price').alias('highest_spent_category'),
        F.avg('total_amount').alias('avg_order_value'),
        F.max('total_amount').alias('max_order_value')
    )
    result.write.jdbc(url=MYSQL_JDBC_URL, table='user_orders_mart', mode="overwrite", properties=MYSQL_PROPERTIES)


marts = [
    {'task_id': 'product_sales_mart', 'build_func': build_product_sales_mart},
    {'task_id': 'user_orders_mart', 'build_func': build_user_orders_mart},
]

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}

dag = DAG(
    'analytics_marts_dag',
    default_args=default_args,
    description='Построение аналитических витрин',
    schedule_interval='0 1 * * *'
)

# Динамическое создание задач для витрин
for mart in marts:
    task = PythonOperator(
        task_id=mart['task_id'],
        python_callable=mart['build_func'],
        dag=dag
    )
    task
