import os
import datetime
import logging
import threading
from queue import Queue
import time
from threading import Thread
import sys

from confluent_kafka import KafkaException
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer # Used to create the Avro serializer in conf
from confluent_kafka.schema_registry.json_schema import JSONSerializer # Used to create the JSON serializer in conf
from confluent_kafka.serialization import StringSerializer # Used to create a generic serializer in conf

from kp_fraydit.custom_types import flatten_list
from kp_fraydit.custom_errors import CustomError, OfflineError, BrokerOfflineError, RegistryOfflineError
from kp_fraydit.connections.connection import KafkaConnection, get_ip_and_port_from_string
from kp_fraydit.schema.processed_schema import ProcessedSchema
from kp_fraydit.schema.fields import Fields
from kp_fraydit.schema.schema_client import SchemaEngine
from kp_fraydit.root import root_dir

# END IMPORTS ///////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# FILE LOGGER ///////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


logger = logging.getLogger(__name__)

# define file handler and set formatter
current_directory = os.path.dirname(os.path.abspath(__file__))


file_handler = logging.FileHandler(f'{root_dir}/logs/producer.log')
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)


# END FILE LOGGER ///////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# PRODUCER CLASS ////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kConn = KafkaConnection()
eng = SchemaEngine()

class BaseProducer:
    __counter = 0

    def __init__(self, topic_name: str, value_schema: ProcessedSchema = None, key_schema: ProcessedSchema = None, 
                    include_value_fields: list = None, include_key_fields: list = None, preserve_order = True
                ) -> None:

        # Check there is a connection
        if kConn.kafka_broker_listener is None: return None

        # INSTANCE COUNT AND ID /////////////////////////////////////////////////////////////////////////////////////////
        '''
            This counts the actual instances of producers there are and that is used to set the transactional id of producers
            that are set to handle their own production queue. The transactional.id for the idempotent confluent kafka producer
            needs to be unique. So, a class class is used (__counter). Each time an instance is created, this counter is
            incremented. The id is set to the counter plus 100,000. The 100,000 ensures that there can be 99,999 instances of the
            KafkaConnection producers. 
        '''
        BaseProducer.__counter += 1
        self.id = BaseProducer.__counter + 100000

        # ESTABLISH CONNECTION //////////////////////////////////////////////////////////////////////////////////////////

        
        # Try to connect
        tries = 0
        while not kConn.registry_online and not kConn.broker_online:
            print (f'Producer could not initialize. Attempting ({tries}) to connect...')
            time.sleep(1)
            tries += 1
            if tries == 10: 
                raise OfflineError


        # SCHEMA ATTRIBUTES AND METHODS /////////////////////////////////////////////////////////////////////////////////
        # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

        self.__topic_name = topic_name

        # self.__value_fields = []
        # self.__optional_value_fields = set()
        # self.__required_value_fields = set()
        self.__value_record = {}
        if include_value_fields is None: self.__include_value_fields = []
        else: self.__include_value_fields = include_value_fields # List of optional arguments beyond the required arguments to write


        # self.__key_fields = []
        # self.__optional_key_fields = []
        # self.__required_key_fields = []
        self.__key_record = {}
        if include_key_fields is None: self.__include_key_fields = []
        else: self.__include_key_fields = include_key_fields 
        
        
        # CREATE SCHEMAS FOR SERIALIZERS ////////////////////////////////////////////////////////////////////////////////
                
        if not kConn.registry_online: # Check to see if registry is online. Raise error if not. If online, set the registry
            raise RegistryOfflineError(f'Registry self.connectionection to address: {kConn.kafka_registry_listener} failed')
        
        
        self.__current_value_schema = value_schema
        if self.__current_value_schema is None:
            _value_serializer = StringSerializer()
        else:
            if self.__current_value_schema.schema_type == 'AVRO':
                _value_serializer = AvroSerializer(eng, self.__current_value_schema.schema_str)
            elif self.__current_value_schema.schema_type == 'JSON':
                _value_serializer = JSONSerializer(self.__current_value_schema.schema_str, eng)
            else:
                _value_serializer = StringSerializer()
    
        self.__current_key_schema = key_schema
        if self.__current_key_schema is None:
            _key_serializer = StringSerializer()
        else:
            if self.__current_key_schema.schema_type == 'AVRO':
                _key_serializer = AvroSerializer(eng, self.__current_key_schema.schema_str)
            elif self.__current_key_schema.schema_type == 'JSON':
                _key_serializer = JSONSerializer(self.__current_key_schema.schema_str, eng)
            else:
                _key_serializer = StringSerializer()

    
        # CREATE THE KAFKA CONFIGURATION DICTIONARY /////////////////////////////////////////////////////////////////////
        

        if not self.broker_online: # Check to see if the broker is online. Raise error if not. If online, set the broker
            raise BrokerOfflineError(kConn.kafka_broker_listener)
    
        ip, port = get_ip_and_port_from_string(kConn.kafka_broker_listener)
        formatted_address = f'{ip}:{port}'
        self.__kafka_broker_listener = formatted_address
        if self.__current_value_schema is None:
            producer_conf = {
            'bootstrap.servers': formatted_address,
            'value.serializer': None,
            'key.serializer': _key_serializer
            }
        elif self.__current_key_schema is None:
            producer_conf = {
            'bootstrap.servers': formatted_address,
            'value.serializer': _value_serializer,
            'key.serializer': None
            }
        elif self.__current_key_schema is None and self.__current_value_schema is None:
            producer_conf = {
            'bootstrap.servers': formatted_address,
            'value.serializer': None,
            'key.serializer': None
            }
        else:
            producer_conf = {
            'bootstrap.servers': formatted_address,
            'value.serializer': _value_serializer,
            'key.serializer': _key_serializer
            }
        
        self.__conf = producer_conf

    
        # PRODUCING ATTRIBUTES //////////////////////////////////////////////////////////////////////////////////////////
        
        
        self.__preserve_order = False
        self.__producer_thread_running = False
        self.__queue = Queue()

        self.__conf['transactional.id'] = self.id

        self.__preserve_order = preserve_order
    # END INITIALIZATION ////////////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @property
    def queue(self):
        return self.__queue

    @classmethod
    def from_schema_names(cls, topic_name: str, value_schema_name: str = None, key_schema_name: str = None, include_value_fields: list = None, 
                            include_key_fields: list = None) -> object:

        # Set value schema default if not set
        if value_schema_name is not None:
            value_schema_name = value_schema_name # Name of the value schema in kafka registry
        else:
            value_schema_name = f'{topic_name}-value' # if no name is specified, default.

        # Set key schema default is not set
        if key_schema_name is not None:
            key_schema_name = key_schema_name # Name of the key schema in kafka registry
        else:
            key_schema_name = f'{topic_name}-key'

        value_schema = ProcessedSchema(eng.get_latest_schema(value_schema_name), value_schema_name)
        key_schema = ProcessedSchema(eng.get_latest_schema(key_schema_name), key_schema_name)
        
        return cls(topic_name, value_schema, key_schema, include_value_fields, include_key_fields)


    @classmethod
    def from_topic(cls, topic_name: str, include_value_fields: list = None, include_key_fields: list = None, preserve_order=False) -> object:
        '''
        This method only applies to confluent kafkas subject naming strategy: "topic_name"-value "topic_name"-key
        This will fail if another naming strategy is used for schemas.
        '''
        value_schema_name = f'{topic_name}-value' # if no name is specified, default.
        key_schema_name = f'{topic_name}-key'

        value_schema = ProcessedSchema(eng.get_latest_schema(value_schema_name),value_schema_name)
        key_schema = ProcessedSchema(eng.get_latest_schema(key_schema_name), key_schema_name)
        
        return cls(topic_name=topic_name, value_schema=value_schema, key_schema=key_schema, 
                include_value_fields=include_value_fields, include_key_fields=include_key_fields, preserve_order=preserve_order)

        # return cls(topic_name=topic_name, value_schema, key_schema, include_value_fields, include_key_fields, preserve_order=preserve_order)

    # PRODUCER CONFIGURATION ////////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    @property
    def conf(self) -> dict:
        return self.__conf


    @property
    def transactional_id(self) -> int:
        return self.__transactional_id


    @transactional_id.setter
    def transactional_id(self, value: int) -> None:
        self.__transactional_id = value
        self.__conf['transactional.id'] = value


    @property
    def preserve_order(self) -> bool:
        return self.__preserve_order


    @preserve_order.setter
    def preserve_order(self, value: bool) -> None:
        self.__preserve_order = value


    @property
    def topic_name(self) -> str:
        return self.__topic_name


    # END PRODUCER CONFIGURATION ////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    

    # SUBSCRIPTION METHOD TO RETRIEVE UPDATES FROM CONNECTION ///////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    def update(self, arg) -> None:
        pass

    # END SUBSCRIPTION METHOD TO RETRIEVE UPDATES FROM CONNECTION ///////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    

    # KAFKA CONNECTION METHODS AND ATTRIBUTES ///////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    

    @property
    def online(self) -> bool:
        if kConn.broker_online and kConn.registry_online: return True
        
        return False


    @property
    def broker_online(self) -> bool:
        return kConn.broker_online


    @property
    def registry_online(self) -> bool:
        return kConn.registry_online


    # END KAFKA CONNECTION METHODS AND ATTRIBUTES ///////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    

    # VALUE SCHEMA ATTRIBUTES AND METHODS ///////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @property
    def value_record(self) -> dict:
        return self.__value_record

    @value_record.setter
    def value_record(self, value):
        self.__value_record = value


    @property
    def current_value_schema(self) -> ProcessedSchema:
        return self.__current_value_schema


    @property
    def value_fields(self) -> Fields:
        return self.current_value_schema.fields


    @property
    def include_value_fields(self) -> list:
        return self.__include_value_fields


    @include_value_fields.setter
    def include_value_fields(self,value: list) -> None:
        
        if isinstance(value, list): 
            scrubbed_list = []
            for v in value:
                if v in self.current_value_schema.fields.optional.names: 
                    scrubbed_list.append(v)
        self.__include_value_fields = scrubbed_list
        

    
    @property
    def missing_value_field_names(self) -> list:

        # all_fields = []
        # all_fields.extend(self.value_fields.required.names)
        # all_fields.extend(self.include_value_fields)
        all_fields = set()
        all_fields |= set(self.value_fields.required.names)
        all_fields |= set(self.include_value_fields)
        for key in self.__value_record:
            if isinstance(self.__value_record[key], dict): # Check to see if its a record
                for subitem in self.__value_record[key]:
                    if subitem in all_fields: all_fields.remove(subitem)
            else:
                if key in all_fields: all_fields.remove(key)
        
        return all_fields


    def addValueArgs(self, **kwargs) -> None:
        for key, value in kwargs.items():
            nested = False
            if key in self.value_fields.names: # Check to see if the field exists
                # Get the value of the kwarg and convert it to the datatype in the kafka schema
                new_val = self.convert_field_type(value, self.value_fields[key].types)
            # Do not add a none datatype. Let the schema default to null
                if new_val is not None:
                    # Check to see if key has parent
                    if self.value_fields[key].parent is not None: # field has a parent
                        nested = True
                        if self.value_record.get(self.value_fields[key].parent) is None: # parent doesnt exist in record
                            self.__value_record[self.value_fields[key].parent] = {}
                        # create the record
                        record_dict = {key: new_val}
                        self.__value_record[self.value_fields[key].parent][key] = new_val
            
                if not nested and new_val is not None: self.__value_record[key] = new_val
            
        self.__check_arguments()
   
    @property
    def values(self) -> list:
        if self.__current_value_schema.raw_schema is None:
            # values = bytes(self.__value_record.get('value').encode('utf-8'))
            values = self.__value_record.get('value')
            # except: values = None
        else:
            values = dict(self.__value_record)
        return values


    # END VALUE SCHEMA ATTRIBUTES AND METHODS ///////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    # KEY SCHEMA ATTRIBUTES AND METHODS /////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    @property
    def key_record(self) -> dict:
        return self.__key_record


    @property
    def current_key_schema(self) -> ProcessedSchema:
        return self.__current_key_schema
        
    @property
    def key_fields(self) -> Fields:
        return self.current_key_schema.fields


    @property
    def include_key_fields(self) -> list:
        return self.__include_key_fields


    @include_key_fields.setter
    def include_key_fields(self,value: list) -> None:
        if isinstance(value, list): 
            scrubbed_list = []
            for v in value:
                if v in self.current_key_schema.fields.optional.names: 
                    scrubbed_list.append(v)
        self.__include_key_fields = scrubbed_list

   
    @property
    def missing_key_field_names(self) -> list:
        # all_fields = []
        # all_fields.extend(self.key_fields.required.names)
        # all_fields.extend(self.include_key_fields)
        all_fields = set()
        all_fields |= set(self.key_fields.required.names)
        all_fields |= set(self.include_key_fields)
        
        for key in self.__key_record:
            if isinstance(self.__key_record[key], dict): # Check to see if its a record
                for subitem in self.__key_record[key]:
                    if subitem in all_fields: all_fields.remove(subitem)
            else:
                if key in all_fields: all_fields.remove(key)
        
        return all_fields


    def addKeyArgs(self, **kwargs) -> None:
        for key, value in kwargs.items():
            nested = False
            if key in self.key_fields.names: # Check to see if the field exists
                # Get the value of the kwarg and convert it to the datatype in the kafka schema
                new_val = self.convert_field_type(value, self.key_fields[key].types)
            # Do not add a none datatype. Let the schema default to null
                if new_val is not None:
                    # Check to see if key has parent
                    if self.key_fields[key].parent is not None: # field has a parent
                        nested = True
                        # create the record
                        if self.key_fields[key].parent not in self.__key_record.keys(): # test to see if parent is in value record
                            # temp_record = str(self.get_key_field(kwarg)['record'])
                            self.__key_record[self.key_fields[key].parent] = {}
                        # create the record
                        record_dict = {key: new_val}
                        self.__key_record[self.key_fields[key].parent][key] = new_val
                        
                if not nested and new_val is not None: self.__key_record[key] = new_val
            
        self.__check_arguments()
   
        

    @property
    def keys(self) -> dict:

        if self.current_key_schema.raw_schema is None:
            keys = self.__key_record.get('key')
            # except: keys = None
        else:
            keys = dict(self.__key_record)
        return keys
    
    
    # END KEY SCHEMA ATTRIBUTES AND METHODS /////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////


    # UPDATE METHODS FOR SCHEMA /////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

    def update_value_schema(self, value_schema: ProcessedSchema) -> None:
        self.__current_value_schema = value_schema

    def update_key_schema(self, key_schema: ProcessedSchema):
        self.__current_key_schema = key_schema

    # END UPDATE METHODS FOR SCHEMA /////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////


    # FIELD PROCESSING METHODS //////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////


    def get_field_types(self, field_name: str, value_schema: bool=True) -> list:
        '''
         value_schema set to True to value schema fields. False to return key schema fields
        '''
        if value_schema == True:
            results = [sub['type'] for sub in self.value_fields if sub['name'] == field_name]
        else:
            results = [sub['type'] for sub in self.key_fields if sub['name'] == field_name]
        
        return (flatten_list(results))

    @staticmethod
    def convert_field_type(value_to_convert: object, field_types_list=None):
        if field_types_list is None: field_types_list = []        
        
        for field_type in field_types_list:
            
            if field_type == 'double' or field_type == 'float' or field_type == 'number':
                try:
                    # float(value_to_convert)
                    return float(value_to_convert)
                except:
                    print (f'Error converting {value_to_convert} to float')
            elif field_type == 'long' or field_type == 'int' or field_type == 'integer':
                try:
                    return int(value_to_convert)
                except:
                    print (f'Error converting {value_to_convert} to integer')
            elif field_type == 'bytes':
                try:
                    return bytes(value_to_convert)
                except:
                    print (f'Error converting {value_to_convert} to bytes')
            elif field_type == 'boolean':
                if value_to_convert == 1 or value_to_convert == 'true' or value_to_convert == 'True' or value_to_convert == True:
                    return True
                elif value_to_convert == 0 or value_to_convert == 'false' or value_to_convert == 'False' or value_to_convert == False:
                    return False
                else:
                    print (f'Error converting {value_to_convert} to boolean')
            elif field_type =='string':
                try:
                    return str(value_to_convert)
                except:
                    print (f'Error converting {value_to_convert} to string')
            
            elif field_type == 'null':
                pass
        
        # Exit loop
        print (f'Conversion error. The value: {value_to_convert} was not of type: {field_types_list}')
        return None


    # END FIELD PROCESSING METHODS //////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////


    # PRODUCE METHODS AND ATTRIBUTES ////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////

    def __check_arguments(self) -> None:
        '''
            Checks to see if all necessary fields are provided. The record is sent to the corresponding queue.
            If not preverve_order, sends the record to the connection queue. If preserve_order, record is sent to the
            class queue.
        '''
        # print ('MISSING FIELDS /////////////////////////////////////////')
        # print ('')
        # print ('')
        # print ('')
        # print ('')
        # print (self.missing_key_field_names)
        # print (self.missing_value_field_names)
        # print ('')
        # print ('')
        # print ('')
        # print ('')
        # print ('MISSING FIELDS /////////////////////////////////////////')
        
        if len(self.missing_value_field_names) == 0 and len(self.missing_key_field_names) == 0:
            
            if self.preserve_order: self.produce()
            else: self.queue_for_connection()

    
    def queue_for_connection(self) -> None:
        # print ('base_producer.BaseProducer.queue for connection')
        # print (f'configuration: {self.conf}')
        # print ('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        # print (self.values)
        # print (self.keys)
        # print ('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')

        kConn.produce_queue.put([self.topic_name, self.keys, self.values, self.conf])
        self.__value_record.clear()  # Delete the records to start the add arguments from scratch    
        self.__key_record.clear()


    def produce(self) -> None:
        if not self.__producer_thread_running: 
            worker = Thread(target=self.__queue_production_loop, args=(), daemon=True)
            worker.start()        
            self.__producer_thread_running = True

        
        self.__queue.put([self.__topic_name, self.keys, self.values, self.__conf])
        self.__value_record.clear()
        self.__key_record.clear()    

    
    def __queue_production_loop(self) -> None:
        prod = SerializingProducer(self.conf)
        prod.init_transactions()
        
        while True:
            task = self.__queue.get()
            topic, values, keys,  conf_ = task # unpack into variables
            
            try:
                prod.begin_transaction()
                prod.produce(topic, values, keys)
            
                prod.commit_transaction()
                prod.flush()
                
            except KafkaException as e:
                if e.args[0].retriable():
                    continue # retry the transaction until an abortable failure happens
                    '''
                    Upon transaction failure or unidentified failure, the transaction is requeued to be processed
                    '''
                elif e.args[0].txn_requires_abort():
                    prod.abort_transaction()
                    self.__queue.put([self.__topic_name, values, keys, self.__conf])
                else:
                    prod.abort_transaction()
                    self.__queue.put([self.__topic_name, values, keys, self.__conf])
                
    # END PRODUCE METHODS AND ATTRIBUTES ////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
    def test_produce(self, keys, values):
        prod = SerializingProducer(self.conf)
        
        prod.init_transactions()
        prod.begin_transaction()
        prod.produce(topic=self.topic_name, key=keys, value=values)
        prod.commit_transaction()
# END PRODUCER CLASS ////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
