"""
DAG для генерации данных по заказам
"""
import random
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import database_exists, create_database
from faker import Faker

from models import Base, Users, Products, Orders, OrderDetails, ProductCategories


def create_database_and_tables():
    """
    Функция для создания базы данных заказов и таблиц в ней
    """
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/orders')
    if not database_exists(engine.url):
        create_database(engine.url)  # Создаем БД orders, если её ещё нет

    Base.metadata.drop_all(engine)  # Удаляем существующие таблицы
    Base.metadata.create_all(engine)  # Создаем заново


def populate_tables():
    """
    Функция генерации фейковых данных
    """
    fake = Faker()
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/orders')
    session_builder = sessionmaker(bind=engine)
    with session_builder() as session:
        # Заполнение пользователей
        emails = set()
        users = []
        for _ in range(1500):
            user_email = fake.email()
            # Нужны только уникальные email
            if user_email in emails:
                continue
            emails.add(user_email)
            users.append(
                Users(
                    first_name=fake.first_name(),
                    last_name=fake.last_name(),
                    email=user_email,
                    phone=fake.phone_number(),
                    loyalty_status=random.choice(['Gold', 'Silver', 'Bronze'])
                )
            )
        session.add_all(users)
        session.flush()

        # Заполнение категорий товаров
        product_categories = [
            'Electronics', 'Furniture', 'Clothing', 'Sports', 'Toys', 'Books',
            'Beauty', 'Automotive', 'Groceries', 'Jewelry', 'Home Appliances', 'Health',
            'Pet Supplies', 'Gardening', 'Office Supplies', 'Food & Beverage', 'Music',
            'Movies', 'Travel', 'Tools', 'Outdoor', 'Baby Products', 'Photography',
            'Art', 'Crafts', 'Gaming', 'Technology', 'Furniture & Decor'
        ]
        categories = [ProductCategories(name=random.choice(product_categories)) for _ in range(50)]
        session.add_all(categories)
        session.flush()

        # Заполнение товаров
        products = [
            Products(
                name=fake.word(),
                description=fake.text(max_nb_chars=200),
                category_id=random.choice(categories).category_id,
                price=round(random.uniform(10, 1000), 2),
                stock_quantity=random.randint(10, 500)
            ) for _ in range(1000)
        ]
        session.add_all(products)
        session.flush()

        # Заполнение заказов
        orders = [
            Orders(
                user_id=random.choice(users).user_id,
                total_amount=round(random.uniform(50, 5000), 2),
                status=random.choice(['Pending', 'Completed', 'Shipped'])
            ) for _ in range(1000)
        ]
        session.add_all(orders)
        session.flush()

        # Заполнение деталей заказов
        order_details = [
            OrderDetails(
                order_id=random.choice(orders).order_id,
                product_id=random.choice(products).product_id,
                quantity=random.randint(1, 10),
                price_per_unit=round(random.uniform(5.0, 500.0), 2),
                total_price=round(random.uniform(20.0, 1000.0), 2)
            ) for _ in range(2000)
        ]
        session.add_all(order_details)
        session.commit()


with DAG(
    'create_orders_data',
    description='Создание таблиц и генерация данных для заказов',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_database_and_tables
    )
    populate_tables_task = PythonOperator(
        task_id='populate_tables',
        python_callable=populate_tables
    )
    trigger_replication = TriggerDagRunOperator(
        task_id='trigger_replication',
        trigger_dag_id='replicate_postgres_to_mysql',
        wait_for_completion=True,
        failed_states=["failed"]
    )
    create_tables_task >> populate_tables_task >> trigger_replication
