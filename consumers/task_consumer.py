from kp_fraydit.consumers.base_consumer import BaseConsumer
from kp_fraydit.schema.processed_schema import ProcessedSchema

def msg_received(key, value):
    print (f'my key: {key}')
    print (f'my value: {value}')

# from kp_fraydit.consumers.base_consumer import BaseConsumer
# con = BaseConsumer.from_topic('test', 'test_beginning', True, msg_received)

from kp_fraydit.consumers.data_consumer import DataConsumer
con2 = DataConsumer.from_topic('test', 'test_beginning', msg_received)

class TaskConsumer(BaseConsumer):
    def __init__(self, topic_name: str, value_schema: ProcessedSchema, key_schema: ProcessedSchema, group_id: str, 
                    read_from_beginning: bool=False, on_message: object=None) -> None:
        
        super().__init__(self, topic_name=topic_name, value_schema=value_schema, key_schema=key_schema, group_id=group_id, 
                            read_from_beginning=False, on_message=on_message)
        

    def on_message(self, key, value):
        pass

