from sqlalchemy import create_engine
from sqlalchemy import Table
from sqlalchemy import insert, select, update, delete

from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import DeclarativeBase

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

host = config['DEFAULT']['host']
port = config['DEFAULT']['port']
username = config['database']['username']
password = config['database']['password']
database_name = config['database']['database_name']

# Create a connection to the database
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database_name}', echo=0)

# Create a session factory
Session = sessionmaker(bind=engine)

# Create a session
session = Session()


class Base(DeclarativeBase):
    pass


# mapping with db
class Customer(Base):
    __table__ = Table(
        "customer",
        Base.metadata,
        autoload_with=engine,
    )


# create operation
session.execute(
    insert(Customer),
    [
        {"first_name": "object", "last_name": "edge", "dob": "15.07.2000", "address": "navi mumbai",
         "created_by": "2018-09-24T00:00:00", "updated_by": "2019-09-24T00:00:00"},
        {'first_name': 'O', 'last_name': 'E', 'dob': '15/07/1994', 'address': 'thane',
         'created_by': '2018-09-24T00:00:00', 'updated_by': '2019-09-24T10:20:30'},
    ])

# read operation
res = session.execute(select('*').where(Customer.first_name == "O"))
print(res.all())

# update operation
session.execute(
    update(Customer),
    [
        {'first_name': 'O', 'last_name': 'E', 'dob': '31/07/1994', 'address': 'Walnut Creek, United States',
         'created_by': '1994-07-31T00:00:00', 'updated_by': '2023-09-24T10:20:30'},
    ])

# delete operation
session.execute(delete(Customer).where(Customer.first_name.in_(["object", 'O'])))

# committing changes
session.commit()