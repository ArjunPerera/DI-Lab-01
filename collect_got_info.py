# Collect GOT characters information from GOT API and saving in POSTGRES DB

import psycopg
import requests
import json
import logging

__version__ = "1.0.0"
__author__ = "Arjun Perera"

hostname = 'localhost'
database = 'got'
username = 'got'
password = 'gotadmin'
port_id = 5555

conn = None
cur = None

Base_URL = "https://thronesapi.com"
End_Point = "/api/v2/Characters"

logging.basicConfig(
    level=logging.DEBUG,
    filename='collect_got_info.log',
    filemode='a',
    format="{asctime} {levelname:<8} {message}",
    style='{'
)

# Database Connection

def dbConnection(hName, dbName, uName, pwd, pid):

    conn = psycopg.connect(
        host = hName,
        dbname = dbName,
        user = uName,
        password = pwd,
        port = pid
    )

    return conn

    logging.info('DB Connected')

# Create Table

def createTable(conn):
    
    cur = conn.cursor()

    create_db = '''CREATE TABLE IF NOT EXISTS gotdata(

			id int PRIMARY KEY,
			firstname varchar(150),
			lastname varchar(150),
			fullname varchar(150),
			title varchar(150),
			family varchar(150),
			image varchar(150),
			imageUrl varchar(150)
		)

		'''
    cur.execute(create_db)
    conn.commit()

# Retrieve and save data

def retrieveSaveData(conn, url, endpoint):
    if __name__ == '__main__':

        cur = conn.cursor()

        # Character api information
    
        BASE_URL = url
        END_POINT = endpoint

        # Create Character URL

        character_url = f"{BASE_URL}{END_POINT}"
        logging.info(character_url)

        # Get data from api

        response = requests.get(character_url)
        logging.info(response)

        if response.status_code == 200:
            data = response.json
        else:
            data = ""
            logging.debug('Your request is wrong......')

        logging.info('Saving Data from API to DB')

        # Save data to DB

        for item in data():
            insert_data = 'INSERT INTO gotdata (id, firstname, lastname, fullname, title, family, image, imageUrl) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'
            insert_values = (item["id"], item["firstName"], item["lastName"], item["fullName"], item["title"], item["family"], item["image"], item["imageUrl"])
            cur.execute(insert_data, insert_values)
            conn.commit()
    
        logging.info('Data Saved to DB!')

# Get number of characters per family name

def getCount(conn):

    cur = conn.cursor()
    cur.execute('select family, count(*) from gotdata group by family')
    for record in cur.fetchall():
        print(record)
    logging.info('Number of characters per family name records fetched')

# function calls

try:

    conn = dbConnection(hostname, database, username, password, port_id)
    createTable(conn)
    retrieveSaveData(conn, Base_URL, End_Point)
    getCount(conn)


except Exception as error:
    logging.debug(error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()

