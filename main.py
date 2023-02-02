import configparser

from sqlalchemy import Table
from sqlalchemy import create_engine, text
# from sqlalchemy import insert, select, update, delete
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session

config = configparser.ConfigParser()
config.read('config.ini')

host = config['DEFAULT']['host']
port = config['DEFAULT']['port']
username = config['database']['username']
password = config['database']['password']
database_name = config['database']['database_name']

# Create a connection to the database
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database_name}', echo=0)

# Create a session
session = Session(bind=engine)


class Base(DeclarativeBase):
    pass


# mapping with db
class Customer(Base):
    __table__ = Table(
        "customer",
        Base.metadata,
        autoload_with=engine,
    )

    def __repr__(self) -> str:
        return f'table - customer p-key[{self.first_name} {self.last_name}]'


# create operation
session.execute(text(
    "INSERT INTO customer (first_name, last_name, dob, address, created_by, updated_by) VALUES ('object', 'edge', '15.07.2000', 'navi mumbai', '2018-09-24T00:00:00', '2019-09-24T00:00:00'), ('O', 'E', '15/07/1994', 'thane', '2018-09-24T00:00:00', '2019-09-24T10:20:30')"))

# read operation
res = session.execute(text("SELECT * FROM customer"))

for row in res:
    print(row)

# update operation
session.execute(text(
    "UPDATE customer SET dob='31/07/1994', address='Walnut Creek, United States', created_by='1994-07-31T00:00:00', updated_by='2023-09-24T10:20:30' WHERE first_name='O' and last_name='E'"))

# delete operation
# session.execute(text("DELETE FROM customer WHERE first_name in ('object', 'O')"))

# committing changes
session.commit()
