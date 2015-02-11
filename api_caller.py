# This is a rabbitmq worker that collects tasks which are to call a certain API with a certain payload and returns the response to a different queue
# body will be a tuple of the type <HTTP_method>, <URI>, <Headers>, <Payload>

#!/usr/bin/env python
import pika
import requests
import logging
logging.basicConfig()
log = logging.getLogger(__name__)


connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='api_calls_queue', durable=True)
channel.queue_declare(queue='response_queue', durable=True)

log.info(' [*] Waiting for messages. To exit press CTRL+C')

def handle_call(ch, method, properties, call):
    url, body, headers = call
    log.info("got request url=%s, body=%s, headers=%s" % (url, body, headers,))
    response = requests.post(url, data=body, headers=headers)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(handle_call,
                      queue='task_queue')

channel.start_consuming()