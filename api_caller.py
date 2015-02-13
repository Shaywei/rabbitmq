#!/usr/bin/env python

import pika
import requests
import logging
import time
import json

defaults = {'logging': {'levels': {'': 'INFO'}}}
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s+0000 - %(name)s - %(levelname)s - %(message)s')
logging.Formatter.converter = time.gmtime
logging.getLogger('pika').setLevel(logging.WARNING)


log = logging.getLogger("API_Caller")

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.queue_declare(queue='api_calls_queue', durable=True)

connection2 = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel2 = connection.channel()
channel2.queue_declare(queue='response_queue', durable=True)

# r = requests.post("http://www.example.com", data=None, headers={})
# print r.status_code
# print r.text

log.info(' [*] Waiting for messages. To exit press CTRL+C')


def _parse_call(call):
    try:
        call = json.loads(call)
    except:
        log.error("Cloudn't parse json: %s" % (call,))
        return None
    return (call["url"], call["body"], call["headers"])


def handle_call(ch, method, properties, call):
    call = _parse_call(call)
    ch.basic_ack(delivery_tag=method.delivery_tag)
    if call is None:
        return
    url, body, headers = call
    log.info("got request url=%s, body=%s, headers=%s" % (url, body, headers,))
    try:
        response = requests.post(url, data=body, headers=headers)
    except:
        log.error("Could not get response. Skipping...")
        return
    log.info("Got response: %s, %s" % (response.status_code, response.text,))
    try:
        channel2.basic_publish(exchange='',
                               routing_key='response_queue',
                               body=json.dumps((response.status_code, response.text)),
                               properties=pika.BasicProperties(delivery_mode=2,))
    except Exception as e:
        log.error("Got exception while trying to publish repsponse: %s" % (e,))


channel.basic_qos(prefetch_count=1)
channel.basic_consume(handle_call,
                      queue='task_queue')

channel.start_consuming()
