from kp_fraydit.class_iterators import ClassIterator
import requests

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.admin.kafka_api.consumer_groups import ConsumerGroup, ConsumerGroups
from kp_fraydit.classes import BaseClass

kConn = KafkaConnection()

class Partition(BaseClass):
        
    def __init__(self, cluster_id: str, topic_name: str, id: int, leader_url: int, replicas_url: list) -> None:
        self.__topic_name = topic_name
        self.__cluster_id = cluster_id
        self.__id = id
        self.__leader_url = leader_url
        self.__replicas_url = replicas_url

    def __str__(self) -> str:
        l = []
        l.append('\n')
        l.append(f'#################################\n')
        l.append(f'id: {self.id}')
        l.append(f'cluster_id: {self.cluster_id}')
        l.append(f'topic name: {self.topic_name}')
        l.append(f'leader: {self.leader_url}')
        l.append(f'replicas: {self.replicas_url}')
        l.append(f'\n ################################# \n')
        return '\n'.join(l)
    
    @property
    def id(self):
        return self.__id

    @property
    def cluster_id(self):
        return self.__cluster_id

    @property
    def topic_name(self):
        return self.__topic_name
    
    @property
    def leader_url(self):
        return self.__leader_url

    @property
    def replicas_url(self):
        return self.__replicas_url


def get_partitions(cluster_id: str, topic_name: str) -> list:
    l = []

    r = requests.get(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{cluster_id}/topics/{topic_name}/partitions')
    data = r.json()['data']
    

    for partition in data:
        l.append({'cluster_id': cluster_id, 'topic_name': topic_name, 'partition_id': partition.get('partition_id'), 
                    'leader_url': partition['leader']['related'], 'replicas_url': partition['replicas']['related']})
    
    return l



class Partitions(ClassIterator):
    def __init__(self, cluster_id: str, topic_name: str, group_list=None) -> None:
        l = []
        for item in get_partitions(cluster_id=cluster_id, topic_name=topic_name):
            l.append(Partition(cluster_id=item['cluster_id'], topic_name=item['topic_name'], 
                        id=item['partition_id'], leader_url=item['leader_url'], replicas_url=item['replicas_url']))

        super().__init__(l)

