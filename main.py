import click
import configparser
import logging
import psycopg2

logging.basicConfig(level=logging.INFO)

config = configparser.ConfigParser()
config.read("config.ini")

host = config["DEFAULT"]["host"]
port = config["DEFAULT"]["port"]
username = config["database"]["username"]
password = config["database"]["password"]
database_name = config["database"]["database_name"]

# Create a connection to the database
conn = psycopg2.connect(
    host=host, port=port, user=username, password=password, database=database_name
)

cur = conn.cursor()


def c(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Create operation")
        insert_query = f"INSERT INTO customer (first_name, last_name, dob, address) VALUES ('{first_name}', '{last_name}', '{dob}', '{address}');"
        cur.execute(insert_query)
    except Exception as e:
        print(f"Failed to insert data due to {e}")
        conn.rollback()
    else:
        conn.commit()
        logging.info(f"Committed Create operation")
    finally:
        cur.close()
        conn.close()
        logging.info(f"Closed the connection")


def r(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Read operation")
        select_query = f"SELECT * FROM customer WHERE 1=1"
        if first_name:
            select_query += f" AND first_name = '{first_name}'"
        if last_name:
            select_query += f" AND last_name = '{last_name}'"
        if dob:
            select_query += f" AND dob = '{dob}'"
        if address:
            select_query += f" AND address = '{address}'"
        cur.execute(select_query)
        result = cur.fetchall()
        for row in result:
            print(f"{row[0]} {row[1]} {row[2]} {row[3]}")
    except Exception as e:
        logging.exception(f"Failed to read data due to {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
        logging.info(f"Closed the connection")


def u(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Update operation")
        update_query = f"UPDATE customer SET dob='{dob}', address='{address}' WHERE first_name='{first_name}' AND last_name='{last_name}'"
        cur.execute(update_query)
    except Exception as e:
        logging.exception(f"Failed to update data due to {e}")
        conn.rollback()
    else:
        conn.commit()
        logging.info(f"Committed Update operation")
    finally:
        cur.close()
        conn.close()
        logging.info(f"Closed the connection")


def d(first_name, last_name, dob, address):
    try:
        logging.info(f"Performing Delete operation")
        delete_query = f"DELETE FROM customer WHERE 1=1"
        if first_name:
            delete_query += f" AND first_name = '{first_name}'"
        if last_name:
            delete_query += f" AND last_name = '{last_name}'"
        if dob:
            delete_query += f" AND dob = '{dob}'"
        if address:
            delete_query += f" AND address = '{address}'"
        cur.execute(delete_query)
    except Exception as e:
        logging.exception(f"Failed to delete data due to {e}")
        conn.rollback()
    else:
        conn.commit()
        logging.info(f"Committed Delete operation")
    finally:
        cur.close()
        conn.close()
        logging.info(f"Closed the connection")


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
