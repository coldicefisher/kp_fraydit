import json
import os
import sys

from kp_fraydit.connectors.connector import Connector, default_topic_creation_conf
from kp_fraydit.schema.schema_client import SchemaEngine
from kp_fraydit.admin.admin_engine import AdminEngine
from kp_fraydit.root import root_dir

from kp_fraydit.financial.price_producer import PriceProducer


# conf = {'bootstrap.servers': formatted_address,}
eng = SchemaEngine()
admin = AdminEngine()


class PriceConnector(Connector):
    def __init__(self, topic_name: str, base_value_schema: str='https://fraydit.com/static/schemas/price_data_value.avro', base_key_schema: str=f'https://fraydit.com/static/schemas/price_data_key.txt', topic_creation_conf: dict=default_topic_creation_conf):

        super().__init__(topic_name=topic_name, topic_creation_conf=topic_creation_conf)
        
        # Validate the schemas
        eng.validate_schema(subject_name=self.topic_name, schema_name=self.value_schema_name, base_schema_source=base_value_schema)
        eng.validate_schema(subject_name=self.topic_name, schema_name=self.key_schema_name, base_schema_source=base_key_schema)

        
