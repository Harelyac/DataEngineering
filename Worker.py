import pandas as pd
import json
import pika
import threading
from dbHandler import *

connection = pika.BlockingConnection(
pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


# first check if it's working with pandas, what about big csv files?
def parsing_csv(file_path, conn):
    read_invoices = pd.read_csv(file_path)
    read_invoices.to_sql('INVOICES', conn, if_exists='append', index=False)


def parsing_json(file_path, conn):
    read_invoices = pd.read_json(file_path)
    read_invoices.to_sql('INVOICES', conn, if_exists='append', index=False)


def finishing(method, year):
    message = "completed|" + year

    # here we create another second connection to another queue
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='complete_queue', durable=True)

    # send message in another queue
    channel.basic_publish(exchange='',
                          routing_key='complete_queue',
                          body=message,
                          properties=pika.BasicProperties(
                              delivery_mode=2,  # make message persistent
                          ))


def callback(ch, method, properties, body):
    parts = str(body, 'utf-8').split("|")

    # connect to db and creating table if not exist
    db_path = 'invoices.sqlite'
    query = """CREATE TABLE IF NOT EXISTS {table_name} (
                                                    BillingAddress TEXT,
                                                    BillingCity TEXT,
                                                    BillingCountry TEXT,
                                                    BillingPostalCode INTEGER,
                                                    BillingState TEXT,
                                                    CustomerId INTEGER,
                                                    InvoiceDate NUMERIC, 
                                                    InvoiceId INTEGER PRIMARY KEY,
                                                    Total INTEGER
                                                );""".format(table_name=parts[2])
    db = dbHandler(db_path)
    conn = db.connect()
    db.create(query)



    # check what type of file it is
    if parts[1] == "csv":
        print("{} started handling {}".format(parts[0], parts[0]))
        parsing_csv(parts[0], conn)
        # Acknowledge the message for rabbit to delete the message from first queue permanently
        ch.basic_ack(method.delivery_tag)
        year = parts[0].split(".")[1].split("_")[1]
        print(year)
        finishing(method, year)

    elif parts[1] == "json":
        print("{} started handling {}".format(parts[0], parts[0]))
        parsing_json(parts[0], conn)
        # Acknowledge the message for rabbit to delete the message from first queue permanently
        ch.basic_ack(method.delivery_tag)
        year = parts[0].split(".")[1].split("_")[1]
        print(year)
        finishing(method, year)


# each woker get 1 task at a time
channel.basic_qos(prefetch_count=1)

# define consume
channel.basic_consume(queue='task_queue', on_message_callback=callback)

# start consuming any message you get from rabbit
channel.start_consuming()



