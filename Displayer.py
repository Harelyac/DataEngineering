import pika
from dbHandler import *
import matplotlib.pyplot as plt
import numpy as np


connection = pika.BlockingConnection(
pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='complete_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def callback(ch, method, properties, body):
    """
    function get called whenever an ack message is received on the completion queue,
    fetching info from data base based on YEAR of file being complete
    :param ch:
    :param method:
    :param properties:
    :param body:
    :return:
    """
    parts = str(body, 'utf-8').split("|")

    db_path = 'invoices.sqlite'
    query = """select SUM(Total), COUNT(DISTINCT CustomerId) from INVOICES where strftime('%Y', InvoiceDate) = '{year}' group by 
        strftime('%m', InvoiceDate)""".format(year=parts[1])

    db = dbHandler(db_path)
    conn = db.connect()
    result = db.select(query)
    sales, active_customers = zip(*result) # unzipping the tuples into two lists

    time = np.linspace(1, 12, 12)
    plt.plot(time, sales, 'o-', color="red", label="Sales")
    plt.plot(time, active_customers, 'o-', color="red", label="Active Customers")
    plt.grid(True)
    plt.xlabel('Year (months)', fontsize=14)
    plt.legend()
    plt.title(parts[1] + " invoices", fontsize=16)
    print(parts[1])
    plt.show()

    # Acknowledge the message for rabbit to delete the message from first queue permanently
    ch.basic_ack(method.delivery_tag)


# define consume
channel.basic_consume(queue='complete_queue', on_message_callback=callback)

# start consuming any message you get from rabbit
channel.start_consuming()