from kp_fraydit.class_iterators import ClassIterator
import requests
import json

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.classes import BaseClass

kConn = KafkaConnection()

'''
Accesses the registry broker at f'{kConn.kafka_registry_listener}/subjects/'
'''


def get_subjects():
        
    r = requests.get(f'{kConn.kafka_registry_listener}/subjects')
    data = r.json()
    subject_list = data
    subjects = []
    for name in subject_list:
        subjects.append({'name': name})
    return subjects

class Subject(BaseClass):
    def __init__(self, name: str) -> None:
        self.__name = name

    def __str__(self) -> str:
        l = []
        l.append('\n')
        l.append(f'#################################\n')
        l.append(f'name: {self.name}')
        l.append(f'\n ################################# \n')
        return '\n'.join(l)
    
    @property
    def name(self):
        return self.__name
    
class Subjects(ClassIterator):
    
    def __init__(self, group_list=None) -> None:
        
        l = []
        for item in get_subjects():
            l.append(Subject(item['name']))
        super().__init__(l)
        
        
    def __getitem__(self, key) -> object:
        for group in self.objList:
            if group.name == key: return Subject(group.name)

    def delete(self, schema_name: str) -> list:
        r = requests.delete(f'{kConn.kafka_registry_listener}/subjects/{schema_name}')
        r = requests.delete(f'{kConn.kafka_registry_listener}/subjects/{schema_name}?permanent=true')
        data = r.json()
        return data




    
