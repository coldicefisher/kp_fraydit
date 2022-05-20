import socket
import time
import abc
import queue
# from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Process
from threading import Thread
from queue import Queue
import os

from confluent_kafka import SerializingProducer, KafkaException

from kp_fraydit import custom_errors
from kp_fraydit.custom_types import check_int, key_exists
from kp_fraydit.metaclasses import SingletonMeta


# END IMPORTS ///////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# HELPER FUNCTIONS //////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


def check_host_port(host: str, port: int, tag='', tries=3, sleep_interval=1) -> tuple:
    a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    a_socket.settimeout(3)
    location = (host, port)
    actual_tries = 0
    while tries > actual_tries:
        
        try:
            result_of_check = a_socket.connect_ex(location)
            if result_of_check == 0: return True, host, port, tag, time.time()
            actual_tries += 1
            time.sleep(sleep_interval)
        
        except:
            actual_tries += 1
            time.sleep(sleep_interval)
                
    return False, host, port, tag


def get_ip_and_port_from_string(ip_string: str) -> tuple:
    
    stripped_string = ip_string.strip()
    if stripped_string[:7] == 'http://': stripped_string = ip_string[7:]
    if stripped_string[:8] == 'https://': stripped_string = ip_string[8:]
    
    if len(stripped_string.split(':')) > 1: return stripped_string.split(':')[0], stripped_string.split(':')[1]
    else: return stripped_string, ''


def valid_address_format(url: str) -> bool:
    stripped_string = url.strip()
    port = stripped_string.split(':')[-1:][0]
    if not check_int(port): return False
    
    if stripped_string[:7] == 'http://' or stripped_string[:8] == 'https://': 
        return True
    
    return False


# END HELPER FUNCTIONS //////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# PRODUCERNATIVE AND HELPER CLASSES /////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


'''
The actual class that is used to do the produccing. It is transactional and is built to ensure an Exact Once Semantics. 
It is used by the KafkaConnection only and in a threaded manner.
'''

class ProducerNative(SerializingProducer):
    def __init__(self, conf_: dict, tx_id: int) -> None:
        self.__print_debug = False
        self.tx_id = tx_id
        conf_['transactional.id'] = tx_id
        self.__conf = conf_
        super().__init__(conf_)
        # self.queue = Queue()    
        self.__print_debug = False
        

    @property
    def print_debug(self):
        return self.__print_debug

    @print_debug.setter
    def print_debug(self, value):
        self.__print_debug = value


    def produce(self, topic: str, keys: list, values: list) -> None: # Overrides parent
        if self.print_debug: print (f'Producing to: {topic} keys: {keys} values:{values}')        
        self.init_transactions()

        '''
        The infinite loop tries a transaction as long as the error is not abortable. If it is abortable, it is then requeued to the connection
        '''
        while True:
            try:
                self.begin_transaction()
                print (f'producing to: {topic} keys: {keys} values {values}...')
                super().produce(topic, keys, values)
                
                self.commit_transaction()
                self.flush()
                break
            
            except KafkaException as e:
                
                # print ('//////////////////////////// ERROR! //////////////////////////')
                # print ('//////////////////////////////////////////////////////////////')
                # print ('\n')
                # print (e)
                # print (e.args)
                # print (e.args[0])
                # print (e.args[0].retriable)
                
                if e.args[0].retriable():
                    print ('retrying')
                    continue # retry the transaction until an abortable failure happens
                    '''
                    Upon transaction failure or unidentified failure, the transaction is requeued to be processed
                    '''
                elif e.args[0].txn_requires_abort():
                    print (f'aborting1: {topic} {values} {keys}')
                    print (e)
                    self.abort_transaction()
                    KafkaConnection().produce_queue.put([topic, values, keys, self.__conf])
                else:
                    print (f'aborting2: {topic} {values} {keys}')
                    print (e)
                    self.abort_transaction()
                    KafkaConnection().produce_queue.put([topic, values, keys, self.__conf])

    
# END PRODUCERNATIVE AND HELPER CLASSES /////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# CONNECTION CLASS //////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


class KafkaConnection(metaclass=SingletonMeta):

    # INITIALIZATION ////////////////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    def __init__(self, thread_count: int = 250) -> None:
        self.__print_debug = False

        self._kafka_broker_listener = os.environ.get('KAFKA_BROKER_LISTENER')
        if self._kafka_broker_listener is None: self._kafka_broker_listener = "10.100.100.8:9092"
        self._kafka_registry_listener = os.environ.get('KAFKA_REGISTRY_LISTENER')
        if self._kafka_registry_listener is None: self._kafka_registry_listener = "http://10.100.100.8:8081"
        self._kafka_rest_api = os.environ.get('KAFKA_REST_API')
        if self._kafka_rest_api is None: self._kafka_rest_api = 'http://10.100.100.8:8082'
        self._kafka_rest_api_version = os.environ.get('KAFKA_REST_API_VERSION')
        if self._kafka_rest_api_version is None: self._kafka_rest_api_version = 'v3'
        

        self._status = {}
        self.__thread_count = thread_count

        self.__observers = set()
        self.__subject_state = {}
        self. __subject_state['broker_online'] = False
        self.__subject_state['registry_online'] = False
    
        self.__registry_online = False
        self.__broker_online = False

        self.loop_active = True
        self.__check_connections()
        self.__produce_queue = queue.Queue()
        self.__start_producer_queue()

    # END INITIALIZATION ////////////////////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    # KAFKA CONNECTION ATTRIBUTES AND METHODS ///////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    
    @property
    def kafka_broker_listener(self) -> str:
        return self._kafka_broker_listener


    @kafka_broker_listener.setter
    def kafka_broker_listener(self, value: str) -> None:
        ip, port = get_ip_and_port_from_string(value)
        formatted_address = f'{ip}:{port}'
        os.environ['KAFKA_BROKER_LISTENER'] = formatted_address
        self._kafka_broker_listener = formatted_address
        
        
    @property
    def kafka_registry_listener(self) -> str:
        return self._kafka_registry_listener


    @kafka_registry_listener.setter
    def kafka_registry_listener(self, value: str) -> None:
        if not valid_address_format(value):
            print (f'Address supplied ({value}) for kafka registry listener is not valid. Please format with http or https preceding address and a port number')
            return
        os.environ['KAFKA_REGISTRY_LISTENER'] = value
        self._kafka_registry_listener = value

    @property
    def kafka_rest_api(self) -> str:
        return self._kafka_rest_api


    @kafka_rest_api.setter
    def kafka_rest_api_version(self, value: str) -> None:
        
        os.environ['KAFKA_REST_API_VERSION'] = value
        if not value[1] == "/": value = f"/{value}"
        self._kafka_rest_api_version = value

    @property
    def kafka_rest_api_version(self) -> str:
        value = ''
        if not self._kafka_rest_api_version[1] == "/": 
            value = f"/{self._kafka_rest_api_version}"
        else:
            value = self._kafka_rest_api_version
            
        return self._kafka_rest_api_version


    @property
    def subject_state(self) -> dict:
        return self.__subject_state


    @subject_state.setter
    def subject_state(self, value: dict) -> None:
        
        if not isinstance(value, dict): 
            print ('The subject state must be a dict: "registry_online": bool, "broker_online": bool')
            return
            if value.get('broker_online') is None or value.get('registry_online') is None:
                print ('The subject state must be a dict: "registry_online": bool, "broker_online": bool')
                return
        
        previous_state = self.__subject_state
        self.__subject_state['broker_online'] = value['broker_online']
        self.__subject_state['registry_online'] = value['registry_online']
        if previous_state != self.__subject_state:
            self.__notify()
        

    @property
    def broker_online(self) -> bool:
        start_time = time.time()
        online = self.__broker_online
        while not online:
            online = self.__broker_online
            if not online:
                time.sleep(.1)
            else: 
                return True
            if start_time + 10 < time.time(): return False
        return online


    @broker_online.setter
    def broker_online(self, value: bool) -> None:
        if not isinstance(value, bool):
            print ('Broker online must be a boolean')
            return
        self.subject_state = {'broker_online': value, 'registry_online': self.__subject_state['registry_online']}


    @property
    def registry_online(self) -> bool:
        '''
        Create a loop that checks for 10 seconds. the long poll of the ping can take some time
        '''
        start_time = time.time()
        online = self.__registry_online
        while not online:
            # online = (self.subject_state['registry_online'])
            online = self.__registry_online
            if not online:
                time.sleep(.1)
            else: 
                return True
            if start_time + 10 < time.time(): return False
        return online


    @registry_online.setter
    def registry_online(self, value: bool) -> None:
        if not isinstance(value, bool):
            print ('Registry online must be a boolean')
            return
        self.subject_state = {'registry_online': value, 'broker_online': self.__subject_state['broker_online']}


    def __check_conn_loop(self) -> None:
        
        '''
        Checks the status of the connection. This runs only when the connection loop_active is true. 
        It is used in check_connections where it is infinitely looped in a daemon thread.
        '''
        while self.loop_active:
            
            changed = False

            a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            a_socket2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            a_socket.settimeout(10)
            a_socket2.settimeout(10)

            # Kafka broker
            ip, port = get_ip_and_port_from_string(self.kafka_broker_listener)
            location = (str(ip), int(port))
            result_of_check = a_socket.connect_ex(location)
            
            if result_of_check == 0: 
                self.__broker_online = True
            else: 
                self.__broker_online = False
            
            # Kafka registry
            ip2, port2 = get_ip_and_port_from_string(self.kafka_registry_listener)
            location2 = (str(ip2), int(port2))
            result_of_check = a_socket2.connect_ex(location2)
            if result_of_check == 0: 
                self.__registry_online = True
            else: 
                self.__registry_online = False
            

            if self.__broker_online != self.broker_online: changed = True
            if self.__registry_online != self.registry_online: changed = True
                        
            self.subject_state = {'broker_online': self.__broker_online, 'registry_online': self.__registry_online}
            
            if changed: 
                self.__notify()

    def __check_connections(self) -> None: # Infinite loop that runs as a deamon thread to update the broker and registry status
        t = Thread(target=self.__check_conn_loop, args=(), daemon=True)
        t.start()
    
    @property # Check to see if this should exist!!!!!!!!!!!!!!!
    def main_conf(self) -> None:
        if kConn.kafka_broker_listener is None: return ""

        ip, port = get_ip_and_port_from_string(kConn.kafka_broker_listener)
        formatted_address = f'{ip}:{port}'
        {'bootstrap.servers': formatted_address,}
    # END KAFKA CONNECTION ATTRIBUTES AND METHODS ///////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    # OBSERVER ATTRIBUTES AND METHODS ///////////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    def attach(self, observer: object) -> None: # Attcahes subscribers
        observer._subject = self
        self.__observers.add(observer)
        self.__notify()
        
    def detach(self, observer: object) -> None: # Detaches subscribers
        observer._subject = None
        self.__observers.discard(observer)
        
    def __notify(self) -> None: # Notifies subscribers of changes
        
        for observer in self.__observers:
            observer.update(self.subject_state)


    # PRODUCE RECORDS METHODS AND ATTRIBUTES ////////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @property
    def produce_queue(self) -> Queue:
        return self.__produce_queue

    @produce_queue.setter
    def produce_queue(self, value: Queue) -> None:
        self.__produce_queue = value

    @property
    def thread_count(self) -> int:
        '''
        Thread count is used as a unique transactional.id attribute for confluent kafkas idempotent producer.
        Thread count cannot be higher than 99,999. Individual producers use the 100,000 and beyond as their 
        transactional.id
        '''
        if self.__thread_count > 99999: return 99999
        else: return self.__thread_count

    @thread_count.setter
    def thread_count(self, value: int) -> None:
        self.__thread_count = value

    @property
    def print_debug(self):
        return self.__print_debug

    @print_debug.setter
    def print_debug(self, value):
        print_debug = value

    def __produce_record(self, thread_no: int, topic: str, values: list, keys: list, conf_: dict) -> None:
        # print(f'Thread #{thread_no} is doing task #{task} in the queue.')
        #print(f'Trying task {topic} | {value_args} | {key_args} | {value_schema} | {key_schema}')

        prod = ProducerNative(conf_, thread_no)
        prod.produce(topic, values, keys)
        
        #print(f'Thread #{prod.tx_id} finished task #{thread_no} | {topic} | {value_args} | {key_args} | {value_schema} | {key_schema}')
        if self.print_debug:
            print(f'Produced: Thread #{prod.tx_id} finished task #{thread_no} | {topic} | {values} | {keys}')


    def __start_producer_queue(self) -> None:
        '''
        queue_consumer is an infinite loop that runs only when the broker is connected.
            It then starts a specified number of thread (self.thread_count) that consumes the records that are placed 
            in the queue by instances of the Producers. These threads are passed the records to consume. 
            Once all threads are finished, it loops again.
        '''
        def __queue_consumer():        
            
            while True:        
            
                prods = []
                workers = []
                while self.broker_online:
                    
                    for i in range(self.thread_count):
                        task = self.produce_queue.get()
                        
                        topic, values, keys,  conf_ = task
                        prods.append(ProducerNative(conf_=conf_, tx_id=i))             
                        
                        worker = Thread(target=self.__produce_record, args=(i, topic, values, keys, conf_), daemon=False)
                        workers.append(worker)
                        worker.start()

                    for i in range(self.thread_count): 
                        workers[i].join()
                        
        queue_thread = Thread(target=__queue_consumer, args=(), daemon=True)
        queue_thread.start()
            
    # END PRODUCER RECORD METHODS AND ATTRIBUTES ////////////////////////////////////////////////////////////////////////
    # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# END CONNECTION CLASS //////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


# Create the Singleton instance
kConn = KafkaConnection()