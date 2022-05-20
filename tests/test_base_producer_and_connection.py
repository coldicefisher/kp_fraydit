import time
from threading import Thread
from queue import Queue
import random

from kp_fraydit.producers.base_producer import BaseProducer
from kp_fraydit.connections.connection import kConn, KafkaConnection
from kp_fraydit.datetime_functions import utc_now_as_long

# Test imports
from kp_fraydit import custom_types, custom_errors, datetime_functions
from kp_fraydit.schema import schema_client
from kp_fraydit.connections import connection
from kp_fraydit.producers.base_producer import BaseProducer


kConn.kafka_broker_listener = '10.100.100.8:9092'
kConn.kafka_registry_listener = 'http://10.100.100.8:8081'
# Contains original settings for broker and registry listeners
o_broker = kConn.kafka_broker_listener
o_registry = kConn.kafka_registry_listener


def test_singleton_connection_settings_work():
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing singleton functionality on connection settings')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')


    kConn.kafka_broker_listener = '10.100.100.8:9999'
    k = kConn
    print ('Two connection settings references created...')
    print ('Broker is deliberately offline. References are synced...')

    assert kConn.kafka_broker_listener == k.kafka_broker_listener, "Kafka broker listener are not the same for two instances"
    kConn.kafka_registry_listener = 'http://10.100.100.8:9999'
    assert kConn.kafka_registry_listener == k.kafka_registry_listener, "Kafka registry listener are not the same for two instances"    
    print ('Registry is deliberately offline. References are synced...')
    
    kConn.kafka_broker_listener = o_broker
    kConn.kafka_registry_listener = o_registry

    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Test of singleton functionality on connection settings passed!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    return True

def test_singleton_connection_works():
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing of singleton functionality!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')


    kConn.subject_state = {'registry_online': False, 'broker_online': False}
    k = KafkaConnection()
    print ('Two references to connection created. Subject state set to offline...')
    print ('')
    
    # Test if the instances have the same subject
    assert k.subject_state == kConn.subject_state, 'Subjects are not the same for two instances'

    # Test registry online and two instances are same
    k.registry_online = True
    assert k.subject_state == kConn.subject_state, 'Registry online are not the same for two instances'

    # Test broker online and two instances are same
    kConn.broker_online = True
    assert k.subject_state == kConn.subject_state, 'Broker online are not the same for two instances'
    print ('References are synced...')
    print ('')
    
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Test of singleton functionality on connection  passed!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    return True

def test_producer_connection_subscription():
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Test of producer connection subscription')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')

    
    prod = BaseProducer.from_topic('test')
    kConn.kafka_broker_listener = '10.100.100.8:9999'
    while kConn.broker_online:
        print ('Sleeping 1 second while broker goes offline...')
        time.sleep(1)
    
    assert kConn.broker_online == False, 'Broker settings failed testing producer'

    assert prod.broker_online == False, 'Producer was not notified of broker online change'
    assert prod.registry_online == True, "Registry is offline and is not supposed to be"
    assert prod.online == False, "Producer online does not work"
    
    print ('Broker is deliberately set to offline. Subscribed producers are synced...')
    print ('')
    kConn.kafka_registry_listener = 'http://10.100.100.8:9999'
    while kConn.registry_online:
        print ('Sleeping 1 second while registry goes offline...')
        time.sleep(1)

    assert prod.registry_online == False, 'Producer was not notified of registry online change'
    assert prod.online == False, "Producer online does not work"
    print ('Registry is deliberately set to offline. Subscribed producers are synced...')
    print ('')

    kConn.kafka_registry_listener = o_registry
    while not kConn.registry_online:
        print ('Sleeping 1 second while registry is coming online...')
        time.sleep(1)
    
    assert kConn.registry_online, "Connection settings for registry did not come back online testing producer"
    assert prod.registry_online, "Producer registry online failed to come back online"
    print ('Registry is back online. Subscribed producers are synced...')
    print ('')
    kConn.kafka_broker_listener = o_broker
    while not kConn.broker_online:
        print ('Sleeping 1 second while broker is coming online...')
        time.sleep(1)
    
    assert kConn.broker_online, "Connection settings for broker did not come back online testing producer"
    assert prod.broker_online, "Producer broker online failed to come back online"
    print ('Broker is back online. Subscribed producers are synced...')
    print ('')
    assert prod.online, "Producer online failed to come back online"
    print ('Subscribed producers are synced with the connection. The broker and registry are reported status and the producers are receiving the updates. The producers are updating their internal status accordingly....')
    print ('')

    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Test of producer connection subscription passed!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')

def test_AvroAvro_producer_functionality():

    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing producer (Avro key Avro value) functionality')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')

    prod = BaseProducer.from_topic('test')
    
    # Check value fields
    print (prod.value_fields.names)
    print (prod.value_fields.required.names)
    assert prod.value_fields.names == ['open', 'high', 'low', 'close', 'volume', 'nullField', 'sourceNested', 'timestampNested']
    # Check required value fields
    assert prod.value_fields.required.names == ['open', 'high', 'low', 'close', 'volume', 'sourceNested', 'timestampNested']
    # Check optional value fields
    assert prod.value_fields.optional.names == ['nullField']
    # Check missing fields
    assert prod.missing_value_field_names == ['open', 'high', 'low', 'close', 'volume', 'sourceNested', 'timestampNested']
    # Check inclusive fields
    assert prod.include_value_fields == []
    
    # Add optional inclusive field
    prod.include_value_fields = ['nullField']
    # Check that missing fields updated after adding optional inclusive field
    assert prod.missing_value_field_names == ['open', 'high', 'low', 'close', 'volume', 'sourceNested', 'timestampNested', 'nullField']
    print ('Producer attributes tested: value fields, required value fields, optional value fields, missing value fields, inclusives optional value fields. Checked same attributes for names. Attributes functioning...')
    print ('')

    # Try to add incorrect data types
    prod.addValueArgs(open='f',high='f',low='f',close='f',volume=1231243.324,sourceNested=1239, timestampNested='fd')
    
    # Check to see if incorrect data types were stored
    print (prod.missing_value_field_names)
    print (prod.value_record)
    
    assert prod.missing_value_field_names == ['open', 'high', 'low', 'close', 'timestampNested', 'nullField'], 'Incorrect data types were added. Error in handling arguments'
    # Add correct data types
    prod.addValueArgs(open=23.32,high=231.34,low=2134.34,close=213432,volume=1231243,sourceNested='test custom error functionality', timestampNested=12383298)
    # Check missing fields after addition of fields
    assert prod.missing_value_field_names == ['nullField']
    prod.addValueArgs(nullField='test assert')
    # Check missing fields after all value fields have been added
    assert prod.missing_value_field_names == []
    print ('Value fields updated and functioning...')
    print ('')
    
    # Check key fields
    assert prod.key_fields.names== ['key','date','nullField']
    # Check required key fields
    assert prod.key_fields.required.names == ['key', 'date']
    # Check optional key fields
    print (prod.key_fields.names)
    print (prod.key_fields.optional.names)
    assert prod.key_fields.optional.names == ['nullField']
    # Check missing key fields
    print (prod.missing_key_field_names)
    assert prod.missing_key_field_names == ['key', 'date']
    print ('Key fields functioning...')
    print ('')
    
    # Add inclusive key field
    prod.include_key_fields = ['nullField']
    # Check key field after adding optional inclusive key field
    
    assert prod.missing_key_field_names == ['key', 'date', 'nullField']
    prod.addKeyArgs(key='test written',date=123984398)
    assert prod.missing_key_field_names == ['nullField']
    
    print ('Key fields updated and functioning...')
    print ('')
    
    prod.addKeyArgs(nullField='test error handling') # Should produce here
    print ('Producer produced...')
    print ('')
    

    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('Test of producer functionality passed!')
    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    return True

def test_JsonJson_producer_functionality():

    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing producer (Json key Json value) functionality')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    print ('')
    prod = BaseProducer.from_topic('test_key_json_value_json')

    # Test that value fields
    print (prod.value_fields.names)
    assert prod.value_fields.names == ['myField1', 'myField2', 'myField3', 'myField4']
    # Test required fields work
    assert prod.value_fields.required.names == ['myField2', 'myField3']
    # Test optional fields work
    print (prod.value_fields.optional.names)
    assert prod.value_fields.optional.names == ['myField1', 'myField4']
    # Test missing fields work
    assert prod.missing_value_field_names == ['myField2', 'myField3']
    # Test inclusive fields work
    assert prod.include_value_fields == []
    # Add optional inclusive field
    prod.include_value_fields = ['myField1']
    # Test optional inclusive fields updated missing value fields after addition
    assert sorted(prod.missing_value_field_names) == ['myField1', 'myField2', 'myField3']
    print ('Producer attributes tested: value fields, required value fields, optional value fields, missing value fields, inclusives optional value fields. Checked same attributes for names. Attributes functioning...')
    print ('')

    prod.addValueArgs(myField2=1.023, myField3='test functionality')
    # myField1 is optional. It is in the inclusive fields. It should not produce
    assert sorted(prod.missing_value_field_names) == ['myField1']
    
    # Test incorrect data types
    prod.addValueArgs(myField1=777.4543)
    assert sorted(prod.missing_value_field_names) == []
    
    
    
    # Test missing fields after all value fields have been added
    print ('Value fields updated and functioning...')
    print ('')
    
    # Test key fields
    assert sorted(prod.key_fields.names) == ['myField1','myField2','myField3']
    # Test required key fields
    assert prod.key_fields.required.names == ['myField3']
    # Test optional key fields
    assert prod.key_fields.optional.names == ['myField1', 'myField2']
    # Test missing key fields
    assert prod.missing_key_field_names == ['myField3']
    print ('Key fields functioning...')
    print ('')
    
    prod.include_key_fields = ['myField2']
    # Test optional inclusive key fields after addition
    assert sorted(prod.missing_key_field_names) == ['myField2', 'myField3']
    prod.addKeyArgs(myField3='test1')
    assert prod.missing_key_field_names == ['myField2']
    # Test missing key fields working
    print ('Key fields updated and functioning...')
    print ('')
    # Test wrong data type
    prod.addKeyArgs(myField2='777w')
    assert prod.missing_key_field_names == ['myField2']
    prod.addKeyArgs(myField2=777) # Should produce here
    print ('Producer produced...')
    print ('')
    
    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('Test of producer (JsonJson) functionality passed!')
    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    return True


def test_AvroNone_producer_functionality():

    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing producer (None key Avro value) functionality')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    prod = BaseProducer.from_topic('test_key_None')

    # Test that value fields
    
    assert sorted(prod.value_fields.names) == ['myField1']
    # Test required fields work
    
    assert sorted(prod.value_fields.required.names) == []
    # Test optional fields work
    assert prod.value_fields.optional.names == ['myField1']
    # Test missing fields work
    # assert sorted(prod.missing_value_field_names) == ['myField1']
    # Test inclusive fields work
    assert prod.include_value_fields == []
    # Add optional inclusive field
    
    prod.include_value_fields = ['myField1']
    # Test optional inclusive fields updated missing value fields after addition
    assert sorted(prod.missing_value_field_names) == ['myField1']
    print ('')

    prod.addValueArgs(myField1='tre')
    # assert sorted(prod.missing_value_field_names) == []
    
    # Test missing fields after all value fields have been added
    print (prod.value_record)
    print (prod.missing_value_field_names)
    # assert sorted(prod.missing_value_field_names) == ['myField1','myField2','myField3']
    print ('Value fields updated and functioning...')
    print ('')
    
    # Test key fields
    # assert prod.key_fields.names== ['key']
    # Test required key fields
    # assert prod.key_fields.required.names == []
    # Test optional key fields
    # assert prod.key_fields.optional.names == ['key']
    # Test missing key fields
    print ('Key fields functioning...')
    print ('')
    
    prod.include_key_fields = ['key']
    # Test optional inclusive key fields after addition
    assert prod.missing_key_field_names == ['key']
    print ('Key fields updated and functioning...')
    print ('')
    
    prod.addKeyArgs(key='test1 key') # should produce here
    
    # Test missing key fields working
    prod.addValueArgs(myField1=random.randint(1,100), myField2=random.random(), myField3='test')
    
    print ('Producer produced...')
    print ('')
    assert prod.missing_key_field_names == ['key']

    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('Test of producer (None key Avro value) functionality passed!')
    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    return True

def test_NoneJson_producer_functionality():

    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing producer (JSON key None value) functionality')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    prod = BaseProducer.from_topic('test_value_None')

    # Test that value fields

    assert prod.value_fields.names == ['value']
    # Test required fields work
    assert prod.value_fields.required.names == ['value']
    # Test optional fields work
    assert prod.value_fields.optional.names == []
    # Test missing fields work
    assert prod.missing_value_field_names == ['value']
    # Test inclusive fields work
    assert prod.include_value_fields == []
    # Test optional inclusive fields updated missing value fields after addition
    print ('')

    # Test adding bad keys
    prod.addValueArgs(myField2='1.023d', myField3=213, myField1='tre')
    assert prod.missing_value_field_names == ['value']
    prod.addValueArgs(value=777)
    assert prod.missing_value_field_names == []
    
    # Test missing fields after all value fields have been added
    
    print ('Value fields updated and functioning...')
    print ('')
    
    # Test key fields
    assert sorted(prod.key_fields.names) == ['myField1', 'myField2', 'myField3']
    # Test required key fields
    assert sorted(prod.key_fields.required.names) == ['myField1', 'myField2']
    # Test optional key fields
    assert prod.key_fields.optional.names == ['myField3']
    # Test missing key fields
    print ('Key fields functioning...')
    print ('')
    
    prod.include_key_fields = ['myField3']
    # Test optional inclusive key fields after addition
    assert sorted(prod.missing_key_field_names) == ['myField1', 'myField2', 'myField3']
    print ('Key fields updated and functioning...')
    print ('')
    
    # Add incorrect data types
    prod.addKeyArgs(myField1='test1 key') # should produce here
    assert sorted(prod.missing_key_field_names) == ['myField1', 'myField2', 'myField3']
    prod.addKeyArgs(myField1=777, myField2=9.374, myField3='test1 functionality') # should produce here
    # Test missing key fields working
    
    print ('Producer produced...')
    print ('')
    
    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('Test of producer (JSON key None value) functionality passed!')
    print ('//////////////////////////////////////////////////////////////')
    print ('//////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    return True

def test_producer_no_schema():
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing of producer (no schemas)!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    prod1 = BaseProducer.from_topic('test_no_schema')
    # Check the value fields
    assert prod1.value_fields.names == ['value']
    assert prod1.value_fields.required.names == ['value']
    assert prod1.value_fields.optional.names == []
    # Cehck the key fields
    assert prod1.key_fields.names == ['key']
    assert prod1.key_fields.optional.names == ['key']
    assert prod1.key_fields.required.names == []
    # Require key and check attributes
    assert prod1.missing_key_field_names == []
    assert prod1.missing_value_field_names == ['value']
    prod1.include_key_fields = ['key']
    assert prod1.missing_key_field_names == ['key']
    assert prod1.missing_value_field_names == ['value']
    assert prod1.key_fields.required.names == []
    assert prod1.key_fields.optional.names == ['key']
    
    prod1.include_key_fields = ['key']
    assert prod1.missing_key_field_names == ['key']
    assert prod1.include_key_fields == ['key']
    assert prod1.key_fields.optional.names == ['key']
    assert prod1.key_fields.required.names == []

    prod1.addKeyArgs(key='test functionality')
    prod1.addValueArgs(value=777.888) # should producer here
    assert prod1.key_record == {}
    assert prod1.value_record == {}
    assert prod1.missing_key_field_names == ['key']
    assert prod1.missing_value_field_names == ['value']
    
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Testing of producer (no schemas) passed!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    print ('')

    return True

def test_producer_stress_test():
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Stress testing producer class!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    print ('')
    print ('Initializing test topic...')
    prod_avro_avro = BaseProducer.from_topic('test')
    prod_avro_avro.preserve_order = True
    # prod_none_avro = Producer('test_key_None')
    print ('Initializing test_value_None topic...')
    prod_json_none = BaseProducer.from_topic('test_value_None') 
    print ('Initializing test_key_json_value_json topic...')
    prod_json_json = BaseProducer.from_topic('test_key_json_value_json')
    print ('Initializing test_no_schema topic...')
    prod_none_none = BaseProducer.from_topic('test_no_schema')

    prod_none_none.preserve_order = True

    print ("All producers intialized. Running tests...")
    r = 10
    for i in range(r):
        
        print (f'Queue count: {kConn.produce_queue.qsize()} Broker status: {kConn.broker_online}')
        print (f'Queue count: {prod_avro_avro.queue.qsize()} Broker status: {kConn.broker_online}')
        if i == 5: 
            kConn.kafka_broker_listener = '10.100.100.8:9999'
            print ('Taking broker offline. Sleeping 10 seconds...')
            time.sleep(10)
            assert kConn.broker_online == False
            kConn.kafka_broker_listener = o_broker
            print ('Broker back online')
            
            

        prod_avro_avro.addValueArgs(open=random.random(),high=random.random(),low=random.random(),close=random.random(),volume=random.randint(1,1000),timestampNested=random.randint(1, 10000),sourceNested=f'stress test {i}')
        prod_avro_avro.addKeyArgs(key=f'stress test {i} key', date=utc_now_as_long()) #should produce

        prod_json_json.addValueArgs(myField1=random.randint(1, 10000), myField2=random.random(), myField3=f'stress {i}')
        prod_json_json.addKeyArgs(myField1=random.randint(1, 10000), myField2=random.random(), myField3=f'{i} stress test')
        
        # if i % 2 == 0: # Add optional fields to even numbers
        #     # prod_none_avro.addValueArgs(myField1=random.randint(1, 10000), myField2=random.random(), myField3=f'stress test {i} value')
            
        prod_json_none.addKeyArgs(myField1=random.randint(1, 10000), myField2=random.random())
        prod_json_none.addValueArgs(value=i)

        prod_none_none.include_key_fields = ['key']
        prod_none_none.addValueArgs(value=f'value {i}')            
        prod_none_none.addKeyArgs(key=f'key {i}')
            
        # else:
        #     # prod_none_avro.addKeyArgs(key=f'stress test {i} key')
        #     # prod_none_avro.addValueArgs(myField1=random.randint(1, 10000), myField2=random.random(), myField3=f'stress test {i} value')

        prod_json_none.addKeyArgs(myField1=random.randint(1, 10000), myField2=random.random(), myField3=f'stress test {i} key')
        prod_json_none.addValueArgs(value=i)
            
            
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('Stress testing producer class!')
    print ('///////////////////////////////////////////////////////////////')
    print ('///////////////////////////////////////////////////////////////')
    print ('')
    print ('')


# test_singleton_connection_settings_work()
# test_singleton_connection_works()
# test_producer_connection_subscription()
# test_AvroAvro_producer_functionality()
# test_JsonJson_producer_functionality()
#### test_AvroNone_producer_functionality()
# test_NoneJson_producer_functionality()
# test_producer_no_schema()
# test_producer_stress_test()