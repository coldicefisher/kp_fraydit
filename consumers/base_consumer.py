from threading import Thread

from kp_fraydit.classes import BaseClass
from confluent_kafka import DeserializingConsumer
# from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
from confluent_kafka import TopicPartition
# from confluent_kafka.schema_registry import SchemaRegistryClient

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.custom_types import flatten_dict
from kp_fraydit.schema.schema_client import SchemaEngine
from kp_fraydit.schema.processed_schema import ProcessedSchema
from kp_fraydit.metaclasses import SingletonMeta


class BaseConsumer:
    def __init__(self, topic_name: str, value_schema: ProcessedSchema, key_schema: ProcessedSchema, group_id: str, 
                    on_message: object=None,read_from_beginning=False, *callbackArgs) -> None:
        '''
        Add topic value and key arguments. If they are empty, default. Consider whether that needs to be abstracted into a
        funtion under the schema engine. Same code is used in the producer.

        Check the schemas and then set the serializer to the schema for the value and the key.

        1. Once the serializer is set, poll the super to retrieve the data. 
        2. Update the classes dataframe.
        3. Invoke the callback for a new message

        Time series data
        '''
        
        if value_schema.schema_type == 'AVRO':
            value_d = AvroDeserializer(SchemaEngine(), value_schema.schema_str)
        elif value_schema.schema_type == 'JSON':
            value_d = JSONDeserializer(value_schema.schema_str)
        else:
            value_d = StringDeserializer()

        if key_schema.schema_type == 'AVRO':
            key_d = AvroDeserializer(SchemaEngine(), key_schema.schema_str)
            
        elif key_schema.schema_type == 'JSON':
            key_d = JSONDeserializer(key_schema.schema_str)
        else:
            key_d = StringDeserializer()


        # if read_from_beginning:
        #     conf = {'bootstrap.servers': KafkaConnection().kafka_broker_listener, 'group.id': group_id, 'value.deserializer': value_d, 'key.deserializer': key_d, 'auto.offset.reset': 'earliest', 'enable.auto.offset.store': 'false', 'enable.auto.commit': 'false'}
            # conf = {'bootstrap.servers': KafkaConnection().kafka_broker_listener, 'group.id': group_id, 'auto.offset.reset': 'earliest', 'enable.auto.offset.store': 'false', 'enable.auto.commit': 'false'}
        # else:
        if read_from_beginning:
            conf = {'bootstrap.servers': KafkaConnection().kafka_broker_listener, 'group.id': group_id, 'value.deserializer': value_d, 'key.deserializer': key_d, 'auto.offset.reset': 'earliest', 'enable.auto.offset.store': 'true', 'enable.auto.commit': 'true'}
        else:
            conf = {'bootstrap.servers': KafkaConnection().kafka_broker_listener, 'group.id': group_id, 'value.deserializer': value_d, 'key.deserializer': key_d, 'auto.offset.reset': 'latest', 'enable.auto.offset.store': 'false', 'enable.auto.commit': 'false'}
        self.__topic_name = topic_name
        self.__value_schema = value_schema
        self.__key_schema = key_schema
        self.__on_message = on_message
        self.__callbackArgs = callbackArgs
        self.__consumer = DeserializingConsumer(conf)
        
        self.__stop_poll = False
        

    @classmethod
    def from_topic(cls, topic_name,  group_id: str, on_message: object=None, read_from_beginning=False, *callbackArgs) -> object:
        value_schema = ProcessedSchema(SchemaEngine().get_latest_schema(f'{topic_name}-value'),topic_name)
        key_schema = ProcessedSchema(SchemaEngine().get_latest_schema(f'{topic_name}-key'), topic_name)
        return cls(topic_name=topic_name, value_schema=value_schema, key_schema=key_schema, group_id=group_id, on_message=on_message,read_from_beginning=read_from_beginning, *callbackArgs)


    def seek(self, partition: int, offset: int) -> None:
        p = TopicPartition(topic=self.__topic_name, partition=0, offset=offset)
        self.__consumer.seek(p)

    def assign(self, partition_list: list) -> None:
        pass


    def poll(self):
        self.__consumer.subscribe([self.__topic_name])
        
        def poll_loop(self):
            while True: # infinite loop

                while True: # Loop until poll does not fail
                    try:
                        msg = self.__consumer.poll()
                        print (msg.value())
                    except:
                        # del self.__consumer
                        # self.__consumer = DeserializingConsumer(self.__conf)
                        # self.__consumer.subscribe([self.__topic_name])
                        pass

                    #This raises the callback
                    # if self.__on_message is not None: self.__on_message(self.__callbackArgs, flatten_dict(msg.key()), flatten_dict(msg.value()))
                    if self.__on_message is not None: 
                        
                        try: self.__on_message(flatten_dict(msg.key()), flatten_dict(msg.value()))
                        except: pass

                    if self.__stop_poll == True:
                        self.__stop_poll = False
                        return

        # Start the poll loop
        t1 = Thread(target=poll_loop, args=(self,), daemon=True)
        t1.start()


    def stop_poll(self):
        self.__stop_poll = True

        

    @property
    def topic_name(self):
        return self.__topic_name


    @property
    def value_schema(self):
        return self.__value_schema

    
    @property
    def key_schema(self):
        return self.__key_schema


    @property
    def consumer(self):
        return self.__consumer


class FromBeginningConsumer(BaseConsumer):
    pass