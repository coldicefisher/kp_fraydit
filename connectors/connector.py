import json

from kp_fraydit.admin.admin_engine import AdminEngine
from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.producers.base_producer import BaseProducer
from kp_fraydit.schema.schema_client import SchemaEngine


eng = SchemaEngine()
admin = AdminEngine()
kConn = KafkaConnection()

default_topic_creation_conf = {
    'num_partitions': '5',
    'retention_time': '-1',
    'retention_size': '-1',
}

class Connector:
    def __init__(self, topic_name: str, topic_creation_conf: dict=default_topic_creation_conf):
        
        '''
        Check the creation dictionary and default any values that are not present. This makes it easier to initialize
        and set what values the user wants.
        Create a topic if it doesn't exist. Then, connect a producer. The properties
        of the producer will pass as properties to the parent connector that will be most widely used.
        This should be related to the topic, connection attributes, etc. Anything related to the management of 
        the kafka schema and connection.
        The producing should be handled by specifying the producer attribute.
        The consuming should be handled by specifying the consumer attribute
        '''
        try: num_p = int(topic_creation_conf['num_partitions'])
        except: num_p = 5
        try: ret_t = int(topic_creation_conf['retention_time'])
        except: ret_t = -1
        try: ret_s = int(topic_creation_conf['retention_size'])
        except: ret_s = -1
        
        if not admin.topic_exists(topic_name): 
            admin.create_topic(topic_name=topic_name, num_partitions=num_p, retention_time=ret_t, retention_size=ret_s)

        
        self.__producer = BaseProducer(topic_name=topic_name)
        
        # self.consumer = consumer

    @property
    def producer(self):
        return self.__producer

    @property
    def topic_name(self):
        return self.producer.topic_name

    @property
    def value_schema_name(self):
        return self.producer.value_schema_name
  
    
    @property
    def key_schema_name(self):
        return self.producer.key_schema_name
        

    @property
    def broker_online(self):
        return self.producer.broker_online
        

    @property
    def registry_online(self):
        return self.__producer.registry_online
    

    def add_field(self, field_name: str, field_type: str, required: bool=True, notes: str='Field generated with kp_fraydit', value_schema=True):
        if value_schema:
            return eng.alter_field(self.value_schema_name, field_name=field_name, field_type=field_type, required=required, 
            notes=notes)
        else:
            return eng.alter_field(self.key_schema_name, field_name=field_name, field_type=field_type, required=required, 
            notes=notes)

    def alter_field(self, field_name: str, field_type: str, required: bool=True, notes: str='Field generated with kp_fraydit', value_schema=True):
        if value_schema:
            return eng.alter_field(self.value_schema_name, field_name=field_name, field_type=field_type, required=required, 
            notes=notes)
        else:
            return eng.alter_field(self.key_schema_name, field_name=field_name, field_type=field_type, required=required, 
            notes=notes)

    def delete_field(self, field_name: str, value_schema: bool=True):
        if value_schema:
            return eng.delete_field(schema_name=self.value_schema_name, field_name=field_name)
        else:
            return eng.delete_field(schema_name=self.key_schema_name, field_name=field_name)