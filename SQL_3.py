import json
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from models import Base, Publisher, Shop, Book, Stock, Sale

DATABASE_URL = "postgresql://postgres:09121982@localhost:5432/mrazishi_db"
engine = sqlalchemy.create_engine(DATABASE_URL)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('fixtures/tests_data.json', 'r', encoding='utf-8') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]

    if not session.get(model, record.get('pk')):
        session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


def get_sales_by_publisher(publisher_name_or_id):
    query = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale). \
        join(Publisher, Publisher.id == Book.id_publisher). \
        join(Stock, Stock.id_book == Book.id). \
        join(Shop, Shop.id == Stock.id_shop). \
        join(Sale, Sale.id_stock == Stock.id)

    if publisher_name_or_id.isdigit():
        query = query.filter(Publisher.id == int(publisher_name_or_id))
    else:
        query = query.filter(Publisher.name == publisher_name_or_id)

    return query.all()

if __name__ == "__main__":
    publisher_name_or_id = input("Введите имя или идентификатор издателя: ")

    sales = get_sales_by_publisher(publisher_name_or_id)

    for sale in sales:
        print(f"{sale.title} | {sale.name} | {sale.price} | {sale.date_sale.strftime('%d-%m-%Y')}")

session.close()