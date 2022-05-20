from kp_fraydit.admin.kafka_api.subjects import Subjects
from kp_fraydit.admin.kafka_api.brokers import Brokers
import requests

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.admin.kafka_api.consumer_groups import ConsumerGroup, ConsumerGroups
from kp_fraydit.admin.kafka_api.topics import Topic, Topics
from kp_fraydit.classes import BaseClass

kConn = KafkaConnection()


def get_clusters() -> list:

    r = requests.get(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters')
    data = r.json()
    cluster_list = data['data']
    clusters = []
    for item in cluster_list:
        
        cluster_id = item['cluster_id']
        acls = '/'.join(item['acls']['related'].split('/')[3:])
        brokers = '/'.join(item['brokers']['related'].split('/')[3:])
        broker_configs = '/'.join(item['broker_configs']['related'].split('/')[3:])
        consumer_groups = '/'.join(item['consumer_groups']['related'].split('/')[3:])
        topics = '/'.join(item['topics']['related'].split('/')[3:])
        partition_reassignments = '/'.join(item['partition_reassignments']['related'].split('/')[3:])
        c = Cluster(cluster_id, acls, brokers, broker_configs, consumer_groups, topics, partition_reassignments)
        clusters.append(c)
        
    return clusters


def main_cluster():
    return get_clusters()[0]

class Cluster(BaseClass):
    def __init__(self, id: str, acls: str, brokers: str, broker_configs: str, consumer_groups: str, 
                    topics: str, partition_reassignments: str
                ) -> None:
        self.__id = id
        self.__acls_url = acls
        self.__brokers_url = brokers
        self.__broker_configs_url = broker_configs
        self.__consumer_groups_url = consumer_groups
        self.__topics_url = topics
        self.__partition_reassignments_url = partition_reassignments


    def __str__(self) -> str:
        l = []
        l.append('\n')
        l.append(f'#################################\n')
        l.append(f'id: {self.id}')
        l.append(f'acls_url: {self.acls_url}')
        l.append(f'broker_configs_url: {self.broker_configs_url}')
        l.append(f'brokers_url: {self.brokers_url}')
        l.append(f'consumer_groups_url: {self.consumer_groups_url}')
        l.append(f'partition_reassignments_url: {self.partition_reassignments_url}')
        l.append(f'topics_url: {self.topics_url}')
        l.append(f'consumer_groups: (count) {len(self.consumer_groups)}')
        l.append(f'\n ################################# \n')
        return '\n'.join(l)

    
    @property
    def consumer_groups(self) -> ConsumerGroups:
        return ConsumerGroups(self.id)

    @property
    def brokers(self) -> Brokers:
        return Brokers(self.id)

    @property
    def topics(self) -> Topics:
        return Topics(self.id)

    @property
    def subjects(self) -> Subjects:
        return Subjects()

    @property
    def id(self) -> str:
        return self.__id
    
    @property
    def acls_url(self) -> str:
        return f'{kConn.kafka_rest_api}/{self.__acls_url}'
    
    @property
    def brokers_url(self) -> str:
        return f'{kConn.kafka_rest_api}/{self.__brokers_url}'

    @property
    def broker_configs_url(self) -> str:
        return f'{kConn.kafka_rest_api}/{self.__broker_configs_url}'

    @property
    def consumer_groups_url(self) -> str:
        return f'{kConn.kafka_rest_api}/{self.__consumer_groups_url}'
    
    @property
    def topics_url(self) -> str:
        return f'{kConn.kafka_rest_api}/{self.__topics_url}'

    @property
    def partition_reassignments_url(self) -> str:
        return f'{kConn.kafka_rest_api}/{self.__partition_reassignments_url}'


