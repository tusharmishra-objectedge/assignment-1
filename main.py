import click
import configparser
import logging
from sqlalchemy import Table
from sqlalchemy import insert, select, update, delete
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Session

logging.basicConfig(level=logging.CRITICAL)

config = configparser.ConfigParser()
config.read("config.ini")

host = config["DEFAULT"]["host"]
port = config["DEFAULT"]["port"]
username = config["database"]["username"]
password = config["database"]["password"]
database_name = config["database"]["database_name"]

# Create a connection to the database
engine = create_engine(
    f"postgresql://{username}:{password}@{host}:{port}/{database_name}", echo=0
)

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
        return f"table - customer p-key[{self.first_name} {self.last_name}]"


def c(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Create operation")
        session.execute(
            insert(Customer).values(
                first_name=first_name, last_name=last_name, dob=dob, address=address
            )
        )
    except Exception as e:
        logging.exception(f"Failed to insert data due to {e}")
        session.rollback()
    else:
        session.commit()
        logging.info(f"Committed Create operation")
    finally:
        session.close()
        logging.info(f"Closed the session")


def r(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Read operation")
        query = select(Customer)
        if first_name or last_name or dob or address:
            query = select(Customer).where(
                Customer.first_name == first_name if first_name else True,
                Customer.last_name == last_name if last_name else True,
                Customer.dob == dob if dob else True,
                Customer.address == address if address else True,
            )
        cust = session.scalars(query)
        print(cust.all())
    except Exception as e:
        logging.exception(f"Failed to read data due to {e}")
        session.rollback()
    finally:
        session.close()
        logging.info(f"Closed the session")


def u(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Update operation")
        session.execute(
            update(Customer).where(
                Customer.first_name == first_name if first_name else True,
                Customer.last_name == last_name if first_name else True,
            ).values(
                dob=dob, address=address
            )
        )
    except Exception as e:
        logging.exception(f"Failed to update data due to {e}")
        session.rollback()
    else:
        session.commit()
        logging.info(f"Committed Update operation")
    finally:
        session.close()
        logging.info(f"Closed the session")


def d(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Delete operation")
        query = delete(Customer)
        if first_name or last_name or dob or address:
            query = delete(Customer).where(
                Customer.first_name == first_name if first_name else True,
                Customer.last_name == last_name if last_name else True,
                Customer.dob == dob if dob else True,
                Customer.address == address if address else True,
            )
        session.execute(query)
    except Exception as e:
        logging.exception(f"Failed to delete data due to {e}")
        session.rollback()
    else:
        session.commit()
        logging.info(f"Committed Delete operation")
    finally:
        session.close()
        logging.info(f"Closed the session")


if __name__ == "__main__":
    mapping = {"c": c, "r": r, "u": u, "d": d}

    @click.command()
    @click.option("--fun", default="r")
    @click.option("--f_name", type=str, default=None)
    @click.option("--l_name", type=str, default=None)
    @click.option("--dob", type=click.DateTime(formats=["%d-%m-%Y"]), default=None)
    @click.option("--address", type=str, default=None)
    def runner(fun, f_name, l_name, dob, address):
        mapping[fun](f_name, l_name, dob, address)

    runner()
