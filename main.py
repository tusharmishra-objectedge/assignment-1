import configparser

from sqlalchemy import Table
from sqlalchemy import create_engine, text
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


try:
    # create operation
    try:
        session.execute(text(
            "INSERT INTO customer (first_name, last_name, dob, address, created_by, updated_by) VALUES ('object', 'edge', '15.07.2000', 'navi mumbai', '2018-09-24T00:00:00', '2019-09-24T00:00:00'), ('O', 'E', '15/07/1994', 'thane', '2018-09-24T00:00:00', '2019-09-24T10:20:30')"))
    except Exception as e:
        print("Error during create operation:", e)
        session.rollback()

    # read operation
    try:
        res = session.execute(text("SELECT * FROM customer"))
        for row in res:
            print(row)
    except Exception as e:
        print("Error during read operation:", e)
        session.rollback()

    # update operation
    try:
        session.execute(text(
            "UPDATE customer SET dob='31/07/1994', address='Walnut Creek, United States', created_by='1994-07-31T00:00:00', updated_by='2023-09-24T10:20:30' WHERE first_name='O' and last_name='E'"))
    except Exception as e:
        print("Error during update operation:", e)
        session.rollback()

    # delete operation
    try:
        session.execute(text("DELETE FROM customer WHERE first_name in ('object', 'O')"))
    except Exception as e:
        print("Error during delete operation:", e)
        session.rollback()

    # committing changes
    try:
        session.commit()
    except Exception as e:
        print("Error during commit:", e)
        session.rollback()

except Exception as e:
    print("An unknown error occurred:", e)
    session.rollback()

finally:
    session.close()
