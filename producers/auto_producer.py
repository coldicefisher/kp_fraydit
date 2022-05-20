from kp_fraydit.producers.base_producer import BaseProducer
from kp_fraydit.schema.schema_client import SchemaEngine
from kp_fraydit.admin.admin_engine import AdminEngine
from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.schema.processed_schema import ProcessedSchema

from confluent_kafka.schema_registry import Schema

kConn = KafkaConnection()
eng = SchemaEngine()
admin = AdminEngine()

'''
The subject naming strategy is assumed to be confluent kafkas subject naming strategy of "topic"-value and "topic"-key.
If this strategy is not used, then this class is useless
'''
class AutoProducer(BaseProducer):
    def __init__(self, topic_name: str, include_value_fields: list = None, include_key_fields: list = None, preserve_order=True) -> None:
        topics = admin.kafka_cluster.topics
        if not topics.exists(topic_name): topics.create(topic_name)
        
        value_schema_name = f'{topic_name}-value'
        key_schema_name = f'{topic_name}-key'

        value_schema = ProcessedSchema.from_subject_name(value_schema_name)
        key_schema = ProcessedSchema.from_subject_name(key_schema_name)

        super().__init__(topic_name, value_schema=value_schema, key_schema=key_schema, include_value_fields=include_value_fields, include_key_fields=include_key_fields, preserve_order=preserve_order)
        