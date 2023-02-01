import sqlalchemy as sq
import json
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
from settings import postgresql as settings
from sqlalchemy_utils import database_exists, drop_database

BASE = declarative_base()


class db_manager():
    def __init__(self, settings):
        self.user = settings['dbuser']
        self.password = settings['dbpassword']
        self.host = settings['dbhost']
        self.port = settings['dbport']
        self.database = settings['dbname']
    def get_dsn(self):
        dsn = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return dsn
    def get_engine(self):
        engine = sq.create_engine(self.get_dsn())
        return engine
    def check_db(self):
        dsn = self.get_dsn()
        if not database_exists(dsn):
            print(f'\nDatabase "{self.database}" not exists, create DB\n')
            return False
        else:
            print(f'\nDatabase "{self.database}" exists\n')
            return True

    def drop_db(self):
        dsn = self.get_dsn()
        print('\n',dsn,'\n')
        if database_exists(dsn):
            drop_database(dsn)
            print(f'\nDatabase "{self.database}" has been deleted\n')
        else:
            print(f'\nDatabase "{self.database}" not exists, nothing to delete\n')

class Publisher(BASE):
    __tablename__ = 'publisher'
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

class Book(BASE):
    __tablename__ = 'book'
    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=40), nullable=False)
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey('publisher.id'), nullable=False)
    publisher = relationship(Publisher, backref='books')

class Shop(BASE):
    __tablename__ = 'shop'
    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

class Stock(BASE):
    __tablename__ = 'stock'
    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey('book.id'), nullable=False)
    id_shop = sq.Column(sq.Integer, sq.ForeignKey('shop.id'), nullable=False)
    book = relationship(Book, backref='books')
    shop = relationship(Shop, backref='shops')
    count = sq.Column(sq.Integer)

class Sale(BASE):
    __tablename__ = 'sale'
    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float)
    date_sale = sq.Column(sq.Date, nullable=False)
    id_stock = sq.Column(sq.Integer, sq.ForeignKey('stock.id'), nullable=False)
    stock = relationship(Stock, backref='sales')
    count = sq.Column(sq.Integer)


def create_tables(engine):
    BASE.metadata.drop_all(engine)
    BASE.metadata.create_all(engine)


def load_data(file):
    Session = sessionmaker(bind=engine)
    session = Session()
    with open(file, 'r') as f:
        data = json.load(f)
    for object in data:
        model = {
            'publisher': Publisher,
            'shop': Shop,
            'book': Book,
            'stock': Stock,
            'sale': Sale
        }[object.get('model')]
        session.add(model(id=object.get('pk'), **object.get('fields')))
    session.commit()

def requests(engine):
    Session = sessionmaker(bind=engine)
    session = Session()
    input_data = input('Введите имя или идентификатор издателя: ')
    try:
        data = int(input_data)
        q = session.query(Publisher).join(Book).join(Stock).join(Sale).join(Shop).filter(Publisher.id == input_data)
    except:
        q = session.query(Publisher).join(Book).join(Stock).join(Sale).join(Shop).filter(Publisher.name.ilike(f'%{input_data}%'))
    for s in q.all():
        for b in s.books:   
            for a in b.books:
                for c in a.sales:
                    print(b.title,'|', a.shop.name, '|', c.price, '|', c.date_sale)


if __name__ == '__main__':
    manager = db_manager(settings)
    # manager.drop_db()
    if manager.check_db():
        engine = manager.get_engine()
        create_tables(engine)
        load_data('test_data.json')
        requests(engine)