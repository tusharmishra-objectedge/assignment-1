import click
import configparser
import logging
from sqlalchemy import Table
from sqlalchemy import insert
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


@click.command()
@click.option("--first_name", prompt="Your first name", required=True, type=str)
@click.option("--last_name", prompt="Your last name", required=True, type=str)
@click.option(
    "--dob",
    prompt="Your DOB in DD-MM-YY format",
    required=True,
    type=click.DateTime(formats=["%d-%m-%Y"]),
)
def insert_data(first_name, last_name, dob):
    try:
        session.execute(
            insert(Customer).values(first_name=first_name, last_name=last_name, dob=dob)
        )
    except Exception as e:
        logging.critical(f"Failed to insert data due to {e}")
        session.rollback()
    else:
        session.commit()
    finally:
        session.close()


if __name__ == "__main__":
    main()
