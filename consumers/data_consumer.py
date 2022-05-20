from logging import exception
from kp_fraydit.consumers.base_consumer import BaseConsumer
import pandas as pd
from threading import Thread

from kp_fraydit.classes import BaseClass
from confluent_kafka import DeserializingConsumer
from confluent_kafka import TopicPartition

# from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import StringDeserializer
# from confluent_kafka.schema_registry import SchemaRegistryClient

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.custom_types import flatten_dict
from kp_fraydit.schema.schema_client import SchemaEngine
from kp_fraydit.schema.processed_schema import ProcessedSchema
from kp_fraydit.metaclasses import SingletonMeta


class DataConsumer(BaseConsumer):
    def __init__(self, topic_name: str, value_schema: ProcessedSchema, key_schema: ProcessedSchema, group_id: str, 
                    on_message: object=None) -> None:
        
        super().__init__(topic_name=topic_name, value_schema=value_schema, key_schema=key_schema, group_id=group_id, 
                    on_message=on_message)

        # Create the dataframe with the fields
        self.__df = self.new_dataframe()
        self.__on_message = on_message
        
        self.__stop_poll = False

    def assign(self, offset):
        p = TopicPartition(topic=self.topic_name, partition=0, offset=offset)
        self.__consumer.seek(p)


    def new_dataframe(self):
        # Create the dataframe with the fields
        column_names = [f'key.{key}' for key in self.key_schema.fields.names]
        value_column_names = [f'value.{value}' for value in self.value_schema.fields.names]
        column_names.extend(value_column_names)
        return pd.DataFrame(columns=column_names)


    def poll(self):
        self.consumer.subscribe([self.topic_name])
    
        def poll_loop(self):
        
            while True: # infinite loop

                while True: # Loop until poll does not fail
                    try:
                        msg = self.consumer.poll()
                        # msg = super().poll()
                        break
                    
                    except:
                        del self.consumer
                        self.consumer = DeserializingConsumer(self.__conf)
                        self.consumer.subscribe([self.__topic_name])

                '''
                Add to the Pandas dataframe
                iterate over the keys and values of the schema. That is the order in which the dataframe was created. Get the
                value from the dictionary that was returned from the message. Pass that to a list. Then, append that list as a new row
                to the dataframe.
                '''
                
                new_row = []
                flattened_value = flatten_dict(msg.value())
                flattened_key = flatten_dict(msg.key())
                for key in self.key_schema.fields.names:
                    v = flattened_key.get(key)
                    if v is not None: new_row.append(v)
                    else: new_row.append('')
                
                for key in self.value_schema.fields.names:
                    v = flattened_value.get(key)
                    if v is not None: new_row.append(v)
                    else: new_row.append('')

                self.__df.loc[len(self.__df)] = new_row

                
                #This raises the callback
                if self.__on_message is not None: self.__on_message(flattened_key, flattened_value)
        
                if self.__stop_poll:
                    self.__stop_poll = False
                    return

        # Start the poll loop
        t1 = Thread(target=poll_loop, args=(self,), daemon=False)
        t1.start()
    

    def stop_poll(self):
        self.__stop_poll = True


    @property
    def data(self) -> object:
        return self.__df


class Con(BaseConsumer):
    def __init__(self, topic_name: str, value_schema: ProcessedSchema, key_schema: ProcessedSchema, group_id: str, 
                    on_message: object=None) -> None:
    
        super().__init__(topic_name=topic_name, value_schema=value_schema, key_schema=key_schema, group_id=group_id, 
                    on_message=on_message)