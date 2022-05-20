import requests
import json

from kp_fraydit.class_iterators import ClassIterator
from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.classes import BaseClass


kConn = KafkaConnection()

def get_configs(cluster_id: str, topic_name: str) -> list:
        # print (f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{cluster_id}/topics/{topic_name}/configs')
        r = requests.get(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{cluster_id}/topics/{topic_name}/configs')
        
        # error check the status
        if not r.status_code == 200: return []

        data = r.json()['data']
        
        configs = []
        for item in data:
            configs.append({'topic': topic_name, 'name': item['name'], 'value': item['value'],  
                            'is_read_only': item['is_read_only'], 'is_sensitive': item['is_sensitive'], 
                            'synonyms': item['synonyms']})

        return configs


class TopicConfig(BaseClass):
    def __init__(self, cluster_id: str, topic_name: str, name: str, value: str, is_read_only: bool, is_sensitive: bool, synonyms: list)  -> None:
        self.__topic_name = topic_name
        self.__cluster_id = cluster_id
        self.__name = name
        self.__value = value
        self.__is_read_only = is_read_only
        self.__is_sensitive = is_sensitive
        self.__synonyms = synonyms
        
    def __str__(self) -> str:
        l = []
        l.append('\n')
        l.append(f'#################################\n')
        l.append(f'name: {self.name}')
        l.append(f'value: {self.value}')
        l.append(f'\n ################################# \n')
        return '\n'.join(l)


    @property
    def topic_name(self) -> str:
        return self.__topic_name

    @property
    def cluster_id(self) -> str:
        return self.__cluster_id

    @property
    def name(self) -> str:
        return self.__name

    @property
    def is_read_only(self) -> bool:
        return self.__is_read_only

    @property
    def is_sensitive(self) -> bool:
        return self.__is_sensitive

    @property
    def synonyms(self) -> list:
        return self.__synonyms


    @property
    def value(self):
        
        d = [item for item in get_configs(self.cluster_id, self.topic_name) if item.get('name') == self.name]
        if d[0]['value'] is None: return ''
        return (d[0]['value'])
        

    @value.setter
    def value(self, value):
        
        headers = {
            "Content-Type": "application/json"
        }

        payload = {"data": [
                    {
                        "name": self.name, 
                        "value": value
                    },
                ]}

        f = requests.post(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{self.cluster_id}/topics/{self.topic_name}/configs:alter', data=json.dumps(payload), headers=headers)
        self.__init__(self.cluster_id, self.topic_name,self.name, self.value, self.is_read_only, self.is_sensitive, self.synonyms)


    def print_all_configs(self):
        l = []
        l.append('\n')
        for config in get_configs(self.topic_name, self.cluster_id):
            
            l.append(f'#################################\n')
            l.append(f'{config["name"]}: {config["value"]}')
            # l.append(f'\n ################################# \n')
        return '\n'.join(l)


class TopicConfigs(ClassIterator):
    
    def __init__(self, topic_name, cluster_id, group_list=None) -> None:
        self.__topic_name = topic_name
        self.__cluster_id = cluster_id
        l = []
        for item in get_configs(self.cluster_id, topic_name):
            l.append(TopicConfig(cluster_id=cluster_id, topic_name=topic_name, name=item['name'], value=item['value'], 
                                    is_read_only=item['is_read_only'], is_sensitive=item['is_sensitive'], synonyms=item['synonyms']))
        super().__init__(l)
        
        

    def __getitem__(self, key) -> object:
        for config in self.objList:
            if config.name == key: return TopicConfig(name=config.name, value=config.value, topic_name=config.topic_name, 
                                                        cluster_id=config.cluster_id, is_read_only=config.is_read_only, 
                                                        is_sensitive=config.is_sensitive, synonyms=config.synonyms)


    def __str__(self):
        l = []
        l.append('\n')

        for c in get_configs(self.cluster_id, self.topic_name):
            
            l.append(f'#################################\n')
            l.append(f'{c["name"]}: {c["value"]}\n')
            # l.append(f'\n ################################# \n')
        return '\n'.join(l)
            
    @property
    def topic_name(self) -> str:
        return self.__topic_name

    @property
    def cluster_id(self) -> str:
        return self.__cluster_id


    def update_configs(self, **kwargs):
        
        l = []
        for key, value in kwargs.items():
            l.append({key.replace('_', '.'): value})

        headers = {
            "Content-Type": "application/json"
        }

        payload = {"data": l}
        print (payload)

        r = requests.post(f'{kConn.kafka_rest_api}/{kConn.kafka_rest_api_version}/clusters/{self.cluster_id}/topics/{self.topic_name}/configs:alter', data=json.dumps(payload), headers=headers)

        if r.status_code == 204:
            return True
        else:
            return False, r.status_code, r.text

    
