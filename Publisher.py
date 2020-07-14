# just send load request to queue
import os
import pika
import time

directory = './files/'
TABLE_NAME = "INVOICES" # not used yet
THREADS_NUM = 1


def main():
    # here we create 1 tcp connection with 1 channel inside it for the publisher
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='task_queue', durable=True)


    for counter, file in enumerate(os.listdir(directory)):
        message = os.path.join(directory, file) + "|" + file.split(".")[1] + "|" + TABLE_NAME
        # here we publish message where routing key is the name of queue itself
        channel.basic_publish(exchange='',
                              routing_key='task_queue',
                              body=message,
                              properties = pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        print(" [x] Sent task %r" % message)
        time.sleep(5) # sleep for 5 second before each publish
    connection.close()

if __name__ == '__main__':
    main()




