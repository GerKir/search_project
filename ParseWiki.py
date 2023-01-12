import mwclient
import psycopg2
import lxml
from bs4 import BeautifulSoup

def doJob(wiki):
    i = 0
    site = mwclient.Site(wiki)
    tag = "ru" if wiki.startswith("ru") else "en"
    for page in site.random(0, 1000):
        i = i + 1
        print(f"------{i}-------")
        id = page['id']
        title = page['title']
        api = site.get("parse", pageid=id, prop="text", format="json")
        for pageInfo in api.values():
            text = pageInfo.get('text').get("*")
            cleanText = BeautifulSoup(text, "lxml").text
            insertData(title, cleanText, tag)
        if i == 500:
            break
    return i

def insertData(title, text, tag):
    postgres_insert_query = """ INSERT INTO info (topic, text, tag) VALUES (%s,%s,%s)"""
    record_to_insert = (title, text, tag)
    print(f"insert {title}")
    cursor.execute(postgres_insert_query, record_to_insert)

try:
    connection = psycopg2.connect(user="kirill",
                                  password="",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()

    count = doJob("wikipedia.org")
    count += doJob("ru.wikipedia.org")

    connection.commit()
    print(count, "Record inserted successfully")

except (Exception, psycopg2.Error) as error:
    print("Failed to insert record into mobile table", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")