import os
import time
import concurrent.futures
import socket

from ..custom_types import key_exists, check_int


def check_host_port(host: str, port: int, tag='', tries=3, sleep_interval=1):
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


def get_ip_and_port_from_string(ip_string: str):
    
    stripped_string = ip_string.strip()
    if stripped_string[:7] == 'http://': stripped_string = ip_string[7:]
    if stripped_string[:8] == 'https://': stripped_string = ip_string[8:]
    
    if len(stripped_string.split(':')) > 1: return stripped_string.split(':')[0], stripped_string.split(':')[1]
    else: return stripped_string, ''
    



def valid_address_format(url):
    stripped_string = url.strip()
    port = stripped_string.split(':')[-1:][0]
    if not check_int(port): return False
    
    if stripped_string[:7] == 'http://' or stripped_string[:8] == 'https://': 
        return True
    
    return False

class ConnectionSettingsMeta(type):
    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)

        return cls._instance

class ConnectionSettings(metaclass=ConnectionSettingsMeta):
    
    def __init__(self, reping_time=3):
    
        self._kafka_broker_listener = os.environ.get('KAFKA_BROKER_LISTENER')
        self._kafka_registry_listener = os.environ.get('KAFKA_REGISTRY_LISTENER')
        self._status = {}
        self._reping_time = reping_time # Sets the time that a ping is considered valid
        # cls.check_settings()
        

    @property
    def kafka_broker_listener(self):
        return self._kafka_broker_listener


    @kafka_broker_listener.setter
    def kafka_broker_listener(self, value):
        ip, port = get_ip_and_port_from_string(value)
        formatted_address = f'{ip}:{port}'
        os.environ['KAFKA_BROKER_LISTENER'] = formatted_address
        self._kafka_broker_listener = formatted_address
        
        
    @property
    def kafka_registry_listener(self):
        return self._kafka_registry_listener


    @kafka_registry_listener.setter
    def kafka_registry_listener(self, value):
        if not valid_address_format(value):
            print (f'Address supplied ({value}) for kafka registry listener is not valid. Please format with http or https preceding address and a port number')
            return
        os.environ['KAFKA_REGISTRY_LISTENER'] = value
        self._kafka_registry_listener = value
        
        
KafkaConnectionSettings = ConnectionSettings()
