from kp_fraydit.schema.fields import Field, Fields
from kp_fraydit.schema.processed_schema import ProcessedSchema
import psycopg2

schema = ProcessedSchema.from_subject_name('test_validate_4-value')
schema.debug_on = True
schema.validate(
                    'https://fraydit.com/static/schemas/price_data_value.avro',
                    Fields([
                        Field('customField1', ['null', 'integer'], 'added by custom declaration'),
                    ])
                )



# Basic function for creating connection
def postgres_conn(auto_commit = False):
    """ Connect to the PostgreSQL database server """
    conn = None
    # connect to the PostgreSQL server
    #print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(
        host='10.100.100.8',
        database='django_trader', 
        user='postgres', 
        password='postgres',
        port='5432'
    )

    #finally:
    #    if conn is not None:
            #conn.close()
            #print('Database connection closed.')
    return conn
