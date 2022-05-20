import requests
import json

from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import NewTopic

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.metaclasses import SingletonMeta
from kp_fraydit.admin.kafka_api.clusters import Cluster, main_cluster
from kp_fraydit.admin.kafka_api.topics import Topics, Topic
from kp_fraydit.admin.kafka_api.subjects import Subjects, Subject
kConn = KafkaConnection()


class AdminEngine(AdminClient, metaclass=SingletonMeta):
    def __init__(self) -> None:
        # my_server = kConn.kafka_registry_listener.split("/")[2]
        ip, port = kConn.kafka_broker_listener.split(':')
        self.__conf = { "bootstrap.servers": f'{ip}:{port}' }
        super().__init__(self.__conf)


    # @property
    # def topics(self) -> dict:    
    #     return Topics()


    @property
    def cluster(self) -> dict:
        return main_cluster()

    @property
    def kafka_cluster(self) -> dict:
        return main_cluster()
    # @property
    # def subjects(self) -> dict:
    #     return Subjects()
