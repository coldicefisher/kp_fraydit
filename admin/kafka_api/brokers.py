from kp_fraydit.class_iterators import ClassIterator
import requests
import json

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.classes import BaseClass

kConn = KafkaConnection()

def get_brokers(cluster_id):
    
    r = requests.get(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{cluster_id}/brokers')
    data = r.json()
    l = []
    for broker in data['data']:
        l.append({'url': '/'.join(broker['metadata']['self'].split('/')[3:]), 'id': broker['broker_id'], })
    return l

class Broker(BaseClass):
    def __init__(self, id: int, url: str) -> None:
        self.__id = id
        self.__url = url

    def __str__(self) -> str:
        l = []
        l.append('\n')
        l.append(f'#################################\n')
        l.append(f'id: {self.id}')
        l.append(f'url: {self.url}')
        l.append(f'\n ################################# \n')
        return '\n'.join(l)
    
    @property
    def id(self):
        return self.__id

    @property
    def url(self):
        return self.__url
    

class Brokers(ClassIterator):
    
    def __init__(self, cluster_id, group_list=None) -> None:
        l = []
        for item in get_brokers(cluster_id):
            l.append(Broker(item['id'], item['url']))
        
        super().__init__(l)
        