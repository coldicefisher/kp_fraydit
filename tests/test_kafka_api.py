from kp_fraydit.admin.kafka_api.clusters import Cluster, main_cluster
from kp_fraydit.admin.kafka_api.brokers import Brokers, Broker
from kp_fraydit.admin.kafka_api.consumer_groups import ConsumerGroups, ConsumerGroup
from kp_fraydit.admin.kafka_api.consumers import AdminConsumers, AdminConsumer
from kp_fraydit.admin.kafka_api.partitions import Partitions, Partition
from kp_fraydit.admin.kafka_api.subjects import Subjects, Subject
from kp_fraydit.admin.kafka_api.topic_configs import TopicConfigs, TopicConfig
from kp_fraydit.admin.kafka_api.topics import Topics, TopicConfigs

cluster = main_cluster()

def check_api():
    print ('Checking cluster...')
    print (f'Cluster id: {cluster.id}')
    print (f'Brokers: {cluster.brokers}')
    print (f'Consumer groups: {cluster.consumer_groups}')
    print (f'urls...')
    print (cluster.consumer_groups_url)
    print (cluster.acls_url)
    print (cluster.broker_configs_url)
    print (cluster.partition_reassignments_url)
    print (cluster.topics_url)
    print (cluster.brokers_url)

    print ('')
    print ('/////////////////////////////////////n')
    print ('Checking brokers...')
    for broker in cluster.brokers:
        print (broker)
        print (broker.id)
        print (broker.url)
    
    print ('')
    print ('/////////////////////////////////////n')
    print ('Checking consumer groups...')
    for grp in cluster.consumer_groups:
        print (grp)
        print (grp.id)
        print (grp.url)
        print (grp.is_simple)
        print (grp.partition_assignor)
        print (grp.state)
        print (grp.broker_coordinator)
        print (grp.consumers_url)
        print (grp.lag_summary_url)
        
        print ('')
        print ('////////////////////////////////////')
        print ('Checking consumers...')
        for con in grp.consumers:
            print (f'id: {con.id}')
            print (f'url: {con.url}')
            print (f'Instance id: {con.instance_id}')
            print (f'Client id: {con.client_id}')
            print (f'Assignments: {con.assignments}')

def check_api2():
    print ('////////////////////////////////////')
    print ('Listing topics...')

    topics = cluster.topics
    print (topics)
    print ('')
    print ('////////////////////////////////////')
    print ('Listing topics properties...')

    for topic in topics:
        print (f'name: {topic.name}')
        print (f'configs: {topic.configs}')
        print (f'partitions: {topic.partitions}')

    print ('')
    print ('////////////////////////////////////')
    print ('Creating topic test_topic_create...')
    topics.create('test_topic_create')
    print ('Checking to see if test_topic_create exists...')
    assert topics.exists('test_topic_create'), 'Topic was not present. Check to see if it was created...'
    print ('Deleting test_topic_create...')
    topics.delete('test_topic_create')
    print ('Checking to see if test_topic_create was deleted...')
    assert topics.exists('test_topic_create') == False, 'Topic was not deleted...'

        


# check_api()
check_api2()