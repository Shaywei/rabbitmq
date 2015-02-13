#!/usr/bin/env python
'''
  This is a rabbitmq worker that collects tasks which are to call
  a certain API with a certain payload and returns the response to a
  different queue
  body will be a tuple of the type <HTTP_method>, <URI>, <Headers>, <Payload>
'''


import pika
import logging
import time
import json

defaults = {'logging': {'levels': {'': 'INFO'}}}
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s+0000 - %(name)s - %(levelname)s - %(message)s')
logging.Formatter.converter = time.gmtime
logging.getLogger('pika').setLevel(logging.WARNING)

log = logging.getLogger("ResponsePrinter")

QUEUE_NAME = 'response_queue'

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)
log.info('[*] Waiting for messages. To exit press CTRL+C')


def handle_call(ch, method, properties, response):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    try:
        response = json.loads(response)
        status_code, text = response
        log.info("got response. status_code=%s" % (status_code,))
    except Exception as e:
        log.error("Erros parsing json. json=%s, error is: %s" % (response, e,))

channel.basic_qos(prefetch_count=1)
channel.basic_consume(handle_call,
                      queue='response_queue')

channel.start_consuming()
