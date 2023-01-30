from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

host = config['DEFAULT']['host']
port = config['DEFAULT']['port']
username = config['database']['username']
password = config['database']['password']
database_name = config['database']['database_name']

# Create a connection to the database
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database_name}')

# Create a session factory
Session = sessionmaker(bind=engine)

# Create a session
session = Session()
