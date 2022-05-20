from typing import ChainMap
from kp_fraydit.class_iterators import ClassIterator
import requests
import json

from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import AdminClient


from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.classes import BaseClass
from kp_fraydit.admin.kafka_api.partitions import Partition, Partitions
from kp_fraydit.admin.kafka_api.subjects import Subject, Subjects
from kp_fraydit.admin.kafka_api.clusters import main_cluster
from kp_fraydit.admin.kafka_api.topic_configs import TopicConfig, TopicConfigs
from kp_fraydit.admin.kafka_api.brokers import Broker, Brokers

'''
Uses version 2 of the rest api. Needs to be updated to use version 3
'''
kConn = KafkaConnection()


class Topic(BaseClass):
    def __init__(self, name: str) -> None:
        self.__name = name
        self.__configs = None
        self.__partitions = None

    def __str__(self) -> str:
        l = []
        l.append('\n')
        l.append(f'#################################\n')
        l.append(f'name: {self.name}')
        l.append(f'configs: {self.configs}')
        l.append(f'partitions: {self.partitions}')
        l.append(f'\n ################################# \n')
        return '\n'.join(l)
    
    @property
    def name(self):
        return self.__name
    
    @property
    def configs(self):
        return TopicConfigs(self.name)

    
    @property
    def partitions(self):
        if self.__partitions is None: self.__get_partitions
        return self.__partitions


    def __get_partitions(self):
        r = requests.get(f'{kConn.kafka_rest_api}/topics/{self.name}')
        data = r.json()
        partitions = Partitions()

        for partition in data['partitions']:
            partitions.append((partition.get('id'), partition.get('leader'), partition.get('replicas')))
        
        self.__partitions = partitions

def get_topics():
    r = requests.get(f'{kConn.kafka_rest_api}/topics')
    data = r.json()
    topic_list = data
    topics = []
    for name in topic_list:
        topics.append({'name': name})
        
    return topics

class Topics(ClassIterator):
    def __init__(self, group_list=None) -> None:
        self.debug_on = False
        self.debug(f'Initializing Topics')
        
        self.debug(f'First time...')
        l = []
        for item in get_topics():
            l.append(Topic(item['name']))
        
        super().__init__(l)
        

    def debug(self, msg):
        if self.debug_on:
            print (msg)

        
    def __getitem__(self, key) -> object:
        for group in self.objList:
            if group.name == key: return Topic(group.name)


    def create(self, topic_name: str, num_partitions: int = 20, retention_time: int = -1, retention_size: int = -1, replication_factor: int = 3) -> None:
        self.debug(f'Creating topic: {topic_name}')
        if not self.exists(topic_name):
            c = main_cluster()

            '''
            Check that the replication factor is not greater than the brokers. If it is greater than the brokers,
            set it equal to the number of brokers
            '''
            self.debug(f'replication factor: {replication_factor} Broker Count: {len(Brokers())}')
            if replication_factor > len(Brokers()): replication_factor = len(Brokers())
            self.debug(f'replication factor: {replication_factor} Broker Count: {len(Brokers())}')
            headers = {
                "Content-Type": "application/json"
            }

            payload = {
                "topic_name": topic_name,
                "partitions_count": num_partitions,
                "replication_factor": replication_factor,
            }
            self.debug(payload)
            # Create topic
            r = requests.post(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{c.id}/topics', data=json.dumps(payload), headers=headers)
            self.debug(r)
            self.debug(r.status_code)
            self.debug(r.request)
            self.debug(r.reason)
            self.debug(r.headers)
            self.debug(r.text)
            if r.status_code == 201:
                return True
            else:
                return False, r.status_code, r.text
        else:
            self.debug('Topic exists.')
            return False, 0, 'Topic Exists'   

    def delete(self, topic_name: str):
        if self.exists(topic_name):
            self.debug(f'Deleting topic: {topic_name}...')
            c = main_cluster()

            r = requests.delete(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{c.id}/topics/{topic_name}')
            self.debug(f'Deleted')
        try:
            self.debug(f'Deleting subject: {topic_name}-value...')
            Subjects().delete(f'{topic_name}-value')
            self.debug(f'Deleted')
        except Exception as e:
            print (e)

        try:
            self.debug(f'Deleting subject: {topic_name}-key...')
            Subjects().delete(f'{topic_name}-key')
            self.debug(f'Deleted')
        except Exception as e:
            print (e)

        '''
        Force delete any schemas associated
        '''
