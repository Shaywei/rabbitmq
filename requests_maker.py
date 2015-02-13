#!/usr/bin/env python
import pika
import logging
import time
import json

defaults = {'logging': {'levels': {'': 'INFO'}}}
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s+0000 - %(name)s - %(levelname)s - %(message)s')
logging.Formatter.converter = time.gmtime
logging.getLogger('pika').setLevel(logging.WARNING)

log = logging.getLogger("RequestsMaker")

s = json.dumps({"headers": {}, "url": "http://www.example.com", "body": None})

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)

message = s
channel.basic_publish(exchange='',
                      routing_key='task_queue',
                      body=message,
                      properties=pika.BasicProperties(delivery_mode=2,))
log.info(" [x] Sent %r" % (message,))
connection.close()
