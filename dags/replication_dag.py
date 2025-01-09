"""
DAG для репликации данных по заказам из PostgreSQL в MySQL
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("PostgresToMySQLReplication") \
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.5.0.jar,/opt/airflow/jars/mysql-connector-java-8.0.28.jar") \
    .getOrCreate()

POSTGRES_JDBC_URL = "jdbc:postgresql://postgres:5432/orders"
MYSQL_JDBC_URL = "jdbc:mysql://mysql:3306/orders"

POSTGRES_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

MYSQL_PROPERTIES = {
    "user": "airflow",
    "password": "airflow",
    "driver": "com.mysql.cj.jdbc.Driver"
}

TABLES = ["orders", "users", "products", "order_details", "product_categories"]


def replicate_data():
    """
    Функция репликации данных по заказам из PostgreSQL в MySQL
    """
    for table in TABLES:
        df = spark.read.jdbc(url=POSTGRES_JDBC_URL, table=table, properties=POSTGRES_PROPERTIES)
        df.write.jdbc(url=MYSQL_JDBC_URL, table=table, mode="overwrite", properties=MYSQL_PROPERTIES)


with DAG(
    'replicate_postgres_to_mysql',
    description='Репликации данных по заказам из PostgreSQL в MySQL',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
):
    replicate_task = PythonOperator(
        task_id='replicate_data',
        python_callable=replicate_data
    )
    replicate_task
