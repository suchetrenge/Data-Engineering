import argparse
import csv
from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

API_KEY = 'QMAPGRKKRYOJSF74'
ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'LRtU/E/wI7BefZR44R05acwpPeYkgpDlfopczEr14TuhMi3zZBR4pqURdid4MMwz'
BOOTSTRAP_SERVER = 'pkc-lzvrd.us-west4.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'UTFOQPPGWZU4JSAJ'
SCHEMA_REGISTRY_API_SECRET = 'Mg/OrWyfb39Mu1jNf1jFc5mhhCJEDFCoqLflD4YUdcoo7hn78TUKS7mUel6ovIqF'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Order:
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_order(data:dict,ctx):
        return Order(record=data)

    def __str__(self):
        return f"{self.record}"


def main(topic):

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    schema_str = schema_registry_client.get_latest_version("restaurent-take-away-data-value").schema.schema_str
    json_deserializer = JSONDeserializer(schema_str,
                                         from_dict=Order.dict_to_order)

    consumer_conf = sasl_conf()
    consumer_conf.update({
                     'group.id': 'group1',
                     'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)
    consumer.subscribe([topic])
    record_consumed = 0
    with open('output.csv', 'w',newline='') as f:
        w = csv.writer(f)
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                order = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

                if order is not None:
                    print("User record {}: order: {}\n"
                          .format(msg.key(), order))
                    my_dict = order.record
                    if record_consumed == 0:
                        w.writerow(my_dict.keys())
                    w.writerow(my_dict.values())
                    record_consumed += 1
            except KeyboardInterrupt:
                break

    print("Record Consumed by 2nd consumer : ", record_consumed)
    consumer.close()

main("restaurent-take-away-data")