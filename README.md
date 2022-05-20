# kp_fraydit
Python library that manages kafka producing with Exact Once Semantics, parallelized and non-blocking processing, handles JSON and Avro schemas, and handles nesting of records.

Please refer to the documentation.html in the root folder for more details.

There are several classes which need to be refactored. The primary classes that are functioning are the:
**BaseProducer**

**BaseConsumer**

**Connection - THIS IS A SINGLETON CLASS**

**To initialize, you will need to set the KAFKA_REGISTRY_LISTENER, KAFKA_BROKER_LISTENER and API_VERSION in the Connection.py file. This should have initialized from environment variables. This is a BUG.**

////////////////////////////////////////////////////////////////
**BaseProducer**
////////////////////////////////////////////////////////////////

In your code:
//////////////////////////////////////////////// Producer Example 1 ///////////////////////////////////////////////

```    
from kp_fraydit.producers.base_producer import BaseProducer

prod = BaseProducer.from_topic(topic_name='TOPIC_NAME', include_value_fields=['OPTIONAL_FIELD1', 'OPTIONAL_FIELD2'], include_key_fields=['OPTIONAL_KEY_FIELD1'], preserve_order=False)

prod.addValueArgs(OPTIONAL_FIELD1='programmer', OPTIONAL_FIELD2='wants to')
prod.addKeyArgs(OPTIONAL_KEY_FIELD1='include me')
```
//////////////////////////////////////////////// End Producer Example 1 ////////////////////////////////////////////


The producer is just a class that manages the schema and the queuing of an actual producer. **Required arguments: topic_name**

The producer parses Avro and JSON schemas from the registry and stores the required fields. By passing **include_value_fields/include_key_fields** you are instructing the producer not to queue the record until those fields are passed.

Once all required fields (either defined by the schema or by the user) are passed, the record is queued.

The **preserve_order** tells the class what queue to send the record to. If set to True, the class uses a thread that consumes the queue and produces the records. The thread retries failed attempts infinitely. The order is preserved because there is only one producer and one producing thread consuming the class queue.

If the **preserve_order** is set to False, the record is sent to a queue owned by the **Connection** class. This queue is consumed by a collection of consumers in multiple threads. The failed attempts are placed back into the queue. There is no order guarantee because these threads are consuming concurrently.

All of the actual producers, however, are instances of the Transactional Producer of Confluent Kafka's python library. This library is an exact-once semantics. **ALL PRODUCERS, WHETHER PRESERVE ORDER OR NOT, ARE INSTANCES OF TRANSACTIONAL PRODUCER of CONFLUENT KAFKA**

There are a few ways to instantiate this class. If you instantiate this class directly, you would need to pass a ProcessedSchema class as a value schema and a key schema. This, however, is not ideal in most cases. using the **from_topic** class method is the easiest method.

/////////////////////////////////////////////////
**BaseConsumer**
/////////////////////////////////////////////////

This class extends the Confluent Kafka's consumer class. It is similar in usage as the producer.

///////////////////////////////////////////////// Consumer Example 1 //////////////////////////////////////////////////////
```
self.business_consumer = BaseConsumer.from_topic(topic_name=f"businessMessages", group_id=f"businessMessages.{random_uuid()}", on_message=self._receive_business_message, read_from_beginning=False)
```
///////////////////////////////////////////////// End Consumer Example 1 //////////////////////////////////////////////////

This is a real world example from my site, https://www.bizniz.io. I have a websocket that consumes messages that are related to businesses that the user may own. 

**INSTEAD OF USING CONSUMER GROUPS IN MY WEBSOCKET THAT ARE STORED IN REDIS, I USE KAFKA AND FILTER MY MESSAGES TO EACH USER AND THEN AUTHORIZE BASED ON PERMISSIONS FROM THE BACKEND db**

In order to achieve this, I use the BaseConsumer:

group_id = Consumer group. I am passing a random uuid in this REAL WORLD example. In a micro services environment, you would be using multiple server instances. So here, we are using a unique group id so that we can scale our server instances out and each socket connection then receives each message.

on_message = Callback function when message is received. You should pass a callback function here that will be fired when a new message is received.

read_from_beginning = False. This settings instantiates the Consumer with the settings so that it does not store its offset position and starts from the last offset. When set to True, in the instance of a websocket, any previuosly unconsumed message of that group will be passed. If you have business logic that is wired to the commands, you will have a mess.


PUT IT ALL TOGETHER!!!! This is a real world example, based on https://www.bizniz.io, of how this class is used in the websocket...

///////////////////////////////////////////////// WEBSOCKET EXAMPLE /////////////////////////////////////////////////////////////////
``` 
from datetime import datetime
import json
import time
import os
import binascii
import ast
import base64
import logging


from asgiref.sync import sync_to_async, async_to_sync
from threading import Thread
from queue import Queue

from asgiref.sync import async_to_sync

import asyncio
from channels.generic.websocket import WebsocketConsumer, AsyncJsonWebsocketConsumer
from libs.exceptions.auth_exceptions import UserUnauthorizedError
from libs.cass_auth.security import decode_token
from libs.cass_auth.middleware import User
from libs.kp_fraydit.consumers.base_consumer import BaseConsumer
from libs.kp_fraydit.metaclasses import SingletonMeta
from libs.kp_fraydit.admin.admin_engine import AdminEngine
from libs.kp_fraydit.datetime_functions import utc_now_as_long
from libs.uuid.uuid import random_uuid

from userSocket.subscription_status import BusinessSubscription
from userSocket.business_consumer import BusinessConsumer
from userSocket.profile_consumer import ProfileConsumer
from userSocket.search_consumer import SearchConsumer
from userSocket.forms_consumer import FormsConsumer
from libs.kp_fraydit.producers.base_producer import BaseProducer
from libs.exceptions.handle_errors import KafkaLoggingHandler, handleErrors


class userConsumer(AsyncJsonWebsocketConsumer):
    
    @property
    def business_producer(self):
        try:
            return self.__business_producer
            
        except Exception as e:
            self.__business_producer  = BaseProducer.from_topic(topic_name=f"businessMessages", include_value_fields=['command', 'payload'], include_key_fields=['department', 'business'], preserve_order=False)
            return self.__business_producer


    @business_producer.setter
    def business_producer(self, value):
        if isinstance(value, BaseProducer): self.__business_producer = value

    @property
    def produce_queue(self):
        try: return self.__produce_queue
        except: return None

    '''
        THIS FUNCTION IS CALLED IN ANOTHER THREAD AND PRODUCES MESSAGES FROM SUBSCRIBED TOPICS.
    
        All messages are placed into a queue and then consumed by this function. The broadcast received is passed to the consumer as a callback function
    
    
    def _receive_all_users_message(self, key, value):
        print (f'broadcast all: {key} and value: {value}')
        self.produce_queue.put([key, value, "systemWide"])
        

    def _receive_specific_user_message(self, key, value):
        print (f'broadcast private: {key} and value: {value}')
        if key.get('username') == User(scope=self.scope).profile_user.encrypted_username: 
            self.produce_queue.put([key, value, "private"])


    @async_to_sync
    async def _receive_business_message(self, key, value):
        try:
            department = key['department']
            business = key['business']
            command = value['command']
            payload = value['payload']
            profileId = value['profileId']
            createdAt = value['createdAt']
            
            try:
                result = BusinessConsumer().process_message(business=business, department=department, command=command, payloadStr=payload, user=User(scope=self.scope), profileId=profileId, createdAt=createdAt)
            except UserUnauthorizedError: 
                # self.send_unauthorized_message()
                pass
            except Exception as e: 
                print ('Received message exception...')
                print (e)

            if result is not None:
                cmd, msg = result
                if cmd == 'send': 
                    print ('sending message now')
                    await self.send_json(msg)
        except UserUnauthorizedError:
            pass    
    
    async def connect(self):
        await self.accept()
        
        if self.produce_queue is None: self.__produce_queue = Queue() # Must be initialized before the callback funciton is passed to the consumer
        self.subscribe_to_business()
            
    @handleErrors
    def subscribe_to_business(self):
        # Create if not exists
        # if not AdminEngine().cluster.topics.exists(f'businessMessages'):
        #     AdminEngine().cluster.topics.create(topic_name=f"businessMessages", num_partitions=20)
        #     AdminEngine().cluster.topics[f'businessMessages'].create_value_schema(field_list=[{'name': 'command', 'type': 'string'}, {'name': 'payload', 'type': 'string'}, {'name': 'profileId', 'type': 'string'}, {'name': 'createdAt', 'type': 'long'}])
        #     AdminEngine().cluster.topics[f'businessMessages'].create_key_schema(field_list=[{'name': 'business', 'type': 'string'},{'name': 'department', 'type': 'string'}])
            
        self.business_consumer = BaseConsumer.from_topic(topic_name=f"businessMessages", group_id=f"businessMessages.{random_uuid()}", on_message=self._receive_business_message, read_from_beginning=False)
        self.business_consumer.poll()
        # if self.business_producer is None: self.business_producer = BaseProducer(topic_name=f"businessMessages", include_value_fields=['command', 'payload'], include_key_fields=['department', 'business'], preserve_order=True)

    # Echo messages to the client
    async def echo_message(self, message): await self.send_json(message)
    # Send unauthorized message to client
    async def send_unauthorized_message(self): await self.send_json({"key": "system", "command": "unauthorized", "payload": {}})
    # Send error message to client
    async def send_error_message(self): await self.send_json({"key": "system", "command": "application_failed", "payload": {} })

    # Receives the messages and processes commands
    async def receive_json(self, content, **kwargs):
        # try: my_user = User(scope=self.scope)
        # except UserUnauthorizedError: 
        #     self.send_unauthorized_message()        
        #     return

        key = content.get('key')        
        cmd = content.get('command')
        payload = content.get('payload')
        
        await self.echo_message(content)
        
        # try:
        if key == 'system':
            if cmd == 'ping': await self.send_json({'command': 'ping'})
        
        elif key == 'profile':
            if cmd == 'get_profile': await self.send_json(ProfileConsumer().get_profile(scope=self.scope))
            elif cmd == 'commit_profile': await self.send_json(ProfileConsumer().commit_profile(payload=payload, scope=self.scope))
            elif cmd == 'set_default_business': await self.send_json(ProfileConsumer().set_default_business(payload=payload, scope=self.scope))
            
        elif key == 'business':
            # SEND JSON BACK ////////////////////////////////////////
            if cmd == 'check_business_name': await self.send_json(BusinessConsumer().check_business_name(payload=payload))
            # AUTHENTICATED
            elif cmd == 'create_business': await self.send_json(BusinessConsumer().create_business(scope=self.scope, payload=payload))
            
            # AUTHORIZED
            elif cmd == 'get_business_profile': await self.send_json(BusinessConsumer().get_business_profile(payload=payload, user=User(scope=self.scope), required_permissions={'Owner', 'Administrator'}, producer=self.business_producer))
            elif cmd == 'get_business_users': await self.send_json(BusinessConsumer().get_business_users(payload=payload, user=User(scope=self.scope), required_permissions={'Owner', 'Administrator', 'Human Resources', 'Dispatching'}))
            elif cmd == 'get_business_data': await self.send_json(BusinessConsumer().get_business_data(payload=payload, user=User(scope=self.scope), required_permissions={'Employee'}))
            elif cmd == 'replace_business_profile_permissions': await self.send_json(BusinessConsumer().replace_business_profile_permissions(payload=payload, producer=self.business_producer, user=User(self.scope), required_permissions={'Owner', 'Administrator'}))
            elif cmd == 'add_profile_to_business': await self.send_json(BusinessConsumer().add_profile_to_business(payload=payload, user=User(scope=self.scope), required_permissions={'Owner', 'Administrator'}, producer=self.business_producer))
            elif cmd == 'create_unassociated_profile': await self.send_json(BusinessConsumer().create_unassociated_profile(payload=payload, user=User(scope=self.scope), required_permissions={'Owner', 'Administrator'}, producer=self.business_producer))
            elif cmd == 'delete_business_profile': await self.send_json(BusinessConsumer().delete_business_profile(payload=payload, user=User(scope=self.scope), required_permissions={'Owner', 'Administrator'}, producer=self.business_producer))
            elif cmd == 'commit_business_profile': await self.send_json(BusinessConsumer().commit_business_profile(payload=payload, user=User(scope=self.scope), required_permissions={'Owner', 'Administrator'}, producer=self.business_producer))
            elif cmd == 'create_application_template': await self.send_json(BusinessConsumer().create_application_template(payload=payload, user=User(scope=self.scope), required_permissions={'Owner', 'Administrator', 'Human_Resources'}, producer=self.business_producer))
            # elif cmd == 'subscribe_to_business': self.subscribe_to_business(payload.get('businessName'))
        
            
        elif key == 'search':
            if cmd == 'search_profiles': await self.send_json(SearchConsumer().search_profiles(payload=payload))
            elif cmd == 'search_business_profiles': await self.send_json(SearchConsumer().search_business_profiles(payload=payload))
            elif cmd == 'search_emails': await self.send_json(SearchConsumer().search_emails(payload=payload))
        elif key == 'forms':
            if cmd == 'get_doc': await self.send_json(FormsConsumer().get_doc())
                                
    async def disconnect(self, code):
        await self.close(code)
```
/////////////////////////////////////////// END WEBSOCKET //////////////////////////////////////////////////////////////////

In this class, which is used in production, you can see exactly how the BusinessConsumer uses the callback to wire in the messages, filter them out, and then process them. Let's go over it:

    def subscribe_to_business(self):
        self.business_consumer = BaseConsumer.from_topic(topic_name=f"businessMessages", group_id=f"businessMessages.{random_uuid()}", on_message=self._receive_business_message, read_from_beginning=False)
        self.business_consumer.poll()

I am instantiating the consumer and setting it as a property on the socket instance. I do this in case, in the future, I want to test that there is only one instance per socket.

from_topic (best way to instantiate )
topic_name = Your topic
group_id = Consumer group. In this scenario, I want a unique id per "server instance". random_uuid is a library I coded to assist in uuid operations for Cassandra.
on_message = Callback function. **IMPORTANT** This class is useless without this. You must have a callback to perform any routine when a message is received. I will show you the code snippet next.
read_from_beginning = False. I want each socket to begin consuming messages after they are connected. So, this ensures that I only receive new messages.

**self.business_consumer.poll()** This starts the consumption. This is necesary always.


**CALLBACK**
```
    @async_to_sync
    async def _receive_business_message(self, key, value):
        try:
            department = key['department']
            business = key['business']
            command = value['command']
            payload = value['payload']
            profileId = value['profileId']
            createdAt = value['createdAt']
            
            try:
                result = BusinessConsumer().process_message(business=business, department=department, command=command, payloadStr=payload, user=User(scope=self.scope), profileId=profileId, createdAt=createdAt)
            except UserUnauthorizedError: 
                # self.send_unauthorized_message()
                pass
            except Exception as e: 
                print ('Received message exception...')
                print (e)

            if result is not None:
                cmd, msg = result
                if cmd == 'send': 
                    print ('sending message now')
                    await self.send_json(msg)
        except UserUnauthorizedError:
            pass    
```
The callback method needs to contain a key and a value. This is the key and value of the record (message) that is received from Kafka. Then, just write your business logic from there.

SIDE NOTES: 

Because I am using an asynchronous websocket consumer, I need to convert the method to a synchronous method so that I can use the "await self.send_json" method of the class instance. 

There is a decorator that is commented out. @HandleErrors. This wraps the function in a try catch loop and calls the logging module whenever there is an error. The logging module has a custom handler that implements the BaseProducer to write to a log file. I think this is also a great example of this class usage:
```
import logging
import os
import sys
import datetime
import inspect

from kp_fraydit.datetime_functions import today_as_string
from kp_fraydit.root import root_dir

from kp_fraydit.producers.auto_producer import AutoProducer
from kp_fraydit.admin.admin_engine import AdminEngine

from asgiref.sync import sync_to_async, async_to_sync
import asyncio

# setup the logger
logger = logging.getLogger(__name__)


# file_handler = logging.FileHandler(f'{root_dir}/logs/errors_{today_as_string()}.log')
logging.Handler()
formatter    = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
# file_handler.setFormatter(formatter)

# add file handler to logger
# logger.addHandler(file_handler)

# Set logger level to error
logger.setLevel(logging.INFO)
```

///////////////////////////////////////////// Custom Logging Example /////////////////////////////////////////////////////////
```    
class KafkaLoggingHandler(logging.Handler):

    def __init__(self):
        logging.Handler.__init__(self)
        self.setFormatter(formatter)
        # Check to see if there is a kafka topic already and it is formatted correctly
        self.producer = AutoProducer(topic_name='programLogs', include_value_fields=[ 'message',], include_key_fields=['level',], preserve_order=False)
        
        
    def emit(self, record):
        #drop kafka logging to avoid infinite recursion
        if record.name == 'kafka':
            return
        try:
            #use default formatting
            msg = self.format(record)
            #produce message
            self.producer.addValueArgs(message=msg)
            self.producer.addKeyArgs(level=self.level)
        except Exception as e:
            import traceback
            ei = sys.exc_info()
            traceback.print_exception(ei[0], ei[1], ei[2], None, sys.stderr)
            del ei


def handleErrors(func):

    kh = KafkaLoggingHandler()
    kh.setLevel(logging.ERROR)
    # setup the logger
    logger = logging.getLogger(func.__name__)
    logger.addHandler(kh)

    if not inspect.isawaitable(func):
        def inner(*args, **kwargs):
            try:
                results = func(*args, **kwargs)
                return results
            except Exception as e:
                logger.error(e)
                raise
        
    else:
        def inner(*args, **kwargs): return func(*args, **kwargs)
    
    
    return inner
```

Please reach out to me if you have any questions! I cannot guarantee I can get to them as I am super busy trying to break into the Software Engineering field, working kids, my own projects, etc. But I will sure try!
  
I hope this codebase helps someone out there achieve their goals as other contributors have lended to my achievements. Thank you open source community!
