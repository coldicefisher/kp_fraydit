

            
from kp_fraydit.admin.schema_client import SchemaEngine
eng = SchemaEngine()
#current_schema = eng.get_latest_schema('test_key_json_value_json-value')
eng.delete_field('test_key_json_value_json-value', 'myField4')


from kp_fraydit.producers.base_producer import BaseProducer

prod = BaseProducer.from_topic('test')
prod.preserve_order = True
prod.addValueArgs(open=213,high=231,low=213,close=213,volume=21434,sourceNested='hello',timestampNested=4353245)
prod.addKeyArgs(key='new record1', date=3142342)


values = {'open': 213.0, 'high': 231.0, 'low': 213.0, 'close': 213.0, 'volume': 21434, 'nestedField': {'sourceNested': 'hello', 'timestampNested': 4353245}}
keys = {'key': 'bitches and money', 'date': 1238439}
prod.test_produce(keys, values)

from kp_fraydit.schema.processed_schema import ProcessedSchema
from kp_fraydit.schema.schema_client import SchemaEngine

eng = SchemaEngine()
schema = ProcessedSchema(eng.get_latest_schema('test-value'), True)
print (schema.fields.names)
print (schema.fields['timestampNested'])

schema = ProcessedSchema.from_subject_name('test-value')



def msg_received(key, value):
    print (f'my key: {key}')
    print (f'my value: {value}')

# from kp_fraydit.consumers.base_consumer import BaseConsumer
# con = BaseConsumer.from_topic('test', 'test_beginning', msg_received)

from kp_fraydit.consumers.data_consumer import DataConsumer
con2 = DataConsumer.from_topic('test', 'test_beginning_2', msg_received)
con2.assign(0)


from kp_fraydit.custom_types import flatten_dict
values = {'open': 213.0, 'high': 231.0, 'low': 213.0, 'close': 213.0, 'volume': 21434, 'nestedField': {'sourceNested': 'hello', 'timestampNested': 4353245}}
flatten_dict(values)

from kp_fraydit.schema.processed_schema import ProcessedSchema
schema = ProcessedSchema.from_subject_name('test_validate_1-value')
schema.validate('https://fraydit.com/static/schemas/price_data_value.avro')

from kp_fraydit.test_all import test_base_producer_and_connection as t
# t.test_JsonJson_producer_functionality()

# t.test_AvroAvro_producer_functionality()
# t.test_JsonJson_producer_functionality()
# t.test_AvroNone_producer_functionality()
# t.test_NoneJson_producer_functionality()
# t.test_producer_no_schema()
t.test_producer_stress_test()

t.test_producer_no_schema()


from kp_fraydit.producers.base_producer import BaseProducer
prod = BaseProducer.from_topic('test_value_None')

prod.addValueArgs(value='hello')
print (prod.values)



from kp_fraydit.schema.processed_schema import ProcessedSchema

schema = ProcessedSchema.from_subject_name('test_validate_3-key')
schema.validate('https://fraydit.com/static/schemas/price_data_value.avro')




from kp_fraydit.admin.admin_engine import AdminEngine
from kp_fraydit.schema.processed_schema import ProcessedSchema
from kp_fraydit.schema.schema_client import SchemaEngine
schema = ProcessedSchema.from_subject_name('iex_stock_data-value')



from kp_fraydit.admin.admin_engine import AdminEngine
AdminEngine().topics.delete('iex_stock_data')
AdminEngine().topics.create('iex_stock_data')
AdminEngine().topics.delete('iex_stock_data')

AdminEngine().subjects.delete('test_validate_3-value')


from kp_fraydit.admin.admin_engine import AdminEngine
AdminEngine().topics.delete('test_validate_3')


from kp_fraydit.admin.kafka_api.topics import Topic, Topics
t = Topics()['test']
print (t.configs['retention.ms'])
t.configs['retention.ms'].value = 10
print (t.configs['retention.ms'])


{"data": [{"cleanup.policy": "DELETE"}, {"compression.type": "gzip"}]}
l = [{'name': 'not it', 'value': '1', 'topic': 'test'}, {'name': 'it', 'value': '2', 'topic': 'test'}, {'name': 'too far', 'value': '3', 'topic': 'test'}]


from kp_fraydit.admin.kafka_api.topics import Topic, Topics
print (Topics().create('topic_create_9'))

from kp_fraydit.admin.kafka_api.brokers import Broker, Brokers
print (len(Brokers()))


from kp_fraydit.admin.kafka_api.clusters import main_cluster
c = main_cluster()
print (c.consumer_groups)

from kp_fraydit.tests import test_kafka_api

from kp_fraydit.admin.admin_engine import AdminEngine
admin = AdminEngine()
cluster = admin.kafka_cluster
topic = cluster.topics['test']
topic.partitions
