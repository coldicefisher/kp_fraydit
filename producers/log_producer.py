from kp_fraydit.producers.base_producer import BaseProducer
from kp_fraydit.admin.schema_client import SchemaEngine

class LogProducer(BaseProducer):
    def __init__(self):

        # Create the topic if not found
        seng = SchemaEngine()
        try:
            seng.get_latest_schema('logger')
        except:
            attempts = 0
            while True:
                if seng.create_topic: break
                if attempts == 10:
                    raise
            
            # Create the schema
            key_schema = ''
            value_schema = ''
            seng.register_schema('logger', key_schema)

        

        super().__init__('logger')