"""
Модели таблиц
"""
from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()


class Users(Base):
    """
    Модель таблицы пользователей
    """
    __tablename__ = 'users'
    user_id = Column(Integer, primary_key=True, autoincrement=True)
    first_name = Column(String(50), nullable=False)
    last_name = Column(String(50), nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    phone = Column(String(30), nullable=True)
    registration_date = Column(DateTime, default=datetime.utcnow)
    loyalty_status = Column(String(20))


class Products(Base):
    """
    Модель таблицы товаров
    """
    __tablename__ = 'products'
    product_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    description = Column(String(255))
    category_id = Column(Integer, ForeignKey('product_categories.category_id'))
    price = Column(Float, nullable=False)
    stock_quantity = Column(Integer, nullable=False)
    creation_date = Column(DateTime, default=datetime.utcnow)


class Orders(Base):
    """
    Модель таблицы товаров
    """
    __tablename__ = 'orders'
    order_id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey('users.user_id'))
    order_date = Column(DateTime, default=datetime.utcnow)
    total_amount = Column(Float, nullable=False)
    status = Column(String(20))
    delivery_date = Column(DateTime)


class OrderDetails(Base):
    """
    Модель таблицы деталей заказов
    """
    __tablename__ = 'order_details'
    order_detail_id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey('orders.order_id'))
    product_id = Column(Integer, ForeignKey('products.product_id'))
    quantity = Column(Integer, nullable=False)
    price_per_unit = Column(Float, nullable=False)
    total_price = Column(Float, nullable=False)


class ProductCategories(Base):
    """
    Модель таблицы категорий товаров
    """
    __tablename__ = 'product_categories'
    category_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    parent_category_id = Column(Integer, ForeignKey('product_categories.category_id'), nullable=True)
