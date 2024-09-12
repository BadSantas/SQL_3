from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Float, Date
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Publisher(Base):
    __tablename__ = 'publisher'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

class Book(Base):
    __tablename__ = 'book'
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    id_publisher = Column(Integer, ForeignKey('publisher.id'), nullable=False)

    publisher = relationship('Publisher', back_populates='books')

class Stock(Base):
    __tablename__ = 'stock'
    id = Column(Integer, primary_key=True)
    id_book = Column(Integer, ForeignKey('book.id'), nullable=False)
    id_shop = Column(Integer, ForeignKey('shop.id'), nullable=False)

    book = relationship('Book', back_populates='stocks')
    shop = relationship('Shop', back_populates='stocks')

class Shop(Base):
    __tablename__ = 'shop'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    stocks = relationship('Stock', back_populates='shop')

class Sale(Base):
    __tablename__ = 'sale'
    id = Column(Integer, primary_key=True)
    price = Column(Float, nullable=False)
    date_sale = Column(Date, nullable=False)
    id_stock = Column(Integer, ForeignKey('stock.id'), nullable=False)
    count = Column(Integer, nullable=False)

    stock = relationship('Stock', back_populates='sales')

Publisher.books = relationship('Book', order_by=Book.id, back_populates='publisher')
Book.stocks = relationship('Stock', order_by=Stock.id, back_populates='book')
Stock.sales = relationship('Sale', order_by=Sale.id, back_populates='stock')