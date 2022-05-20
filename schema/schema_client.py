import os
import json
import requests
import time

from confluent_kafka.admin import NewTopic
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import Schema

from kp_fraydit.connections.connection import KafkaConnection
from kp_fraydit.metaclasses import SingletonMeta
from kp_fraydit.custom_errors import CustomError, RegistryOfflineError


kConn = KafkaConnection()
schema_compatibility_modes = ['FULL_TRANSITIVE', 'FULL', 'FORWARD_TRANSITIVE', 'FORWARD', 'BACKWARD', 'BACKWARD_TRANSITIVE', 'NONE']
avro_schema_field_types = ['null', 'boolean', 'int', 'long', 'float', 'double', 'string', 'bytes']
json_schema_field_types = ['null', 'integer', 'number', 'string']

class SchemaEngine(SchemaRegistryClient, metaclass=SingletonMeta):

    def __init__(self) -> None:
        '''
        Make sure that enough time is given to ensure that the connection config has connected
        '''
        # start_time = time.time()
        # while True:
        #     if not kConn.registry_online: time.sleep(1)    
        #     else: break
        #     if start_time + 5 < time.time(): raise RegistryOfflineError(kConn.kafka_registry_listener)
        
        if not kConn.registry_online: raise RegistryOfflineError(kConn.kafka_registry_listener)
        schema_client_conf = {'url':f'{kConn.kafka_registry_listener}'}
        self._schema_client = SchemaRegistryClient(schema_client_conf)
        super().__init__(schema_client_conf)
    
        # except RegistryOfflineError as e:
        #     pass
        
        # except Exception as e:
        #     raise CustomError(e)
    
    @property
    def schema_subjects(self):
        return self.get_subjects()


    def get_latest_schema(self, schema_subject: str):
        try:
            return super().get_latest_version(schema_subject).schema
        except:
            return None

    
    # GET VALUE FIELDS //////////////////////////////////////////////////////////////////////////////////////////////////
    
    '''
    Helper function that processes the schema type and sets the required, optional, and all fields for the value schema.
    This function flattens a nested Avro schema and that allows for nesting capabilities. This only works for Avro schema.
    The nested schema needs to be like this:
    fields: [
        {
            "name": "nestedField",
            "type": [
                {
                    "doc": "struture that contains nested info",
                    "name": "TestpathElement",
                    "namespace": "com.example.kafka.core",
                    "type": "record"
                    "fields": [
                        {
                            "name": "sourceNested",
                            "type": "string"
                        },
                        {
                            "name": "timestampNested",
                            "type": "long"
                        }
                    ],
                }
            ]
        }
    ]
    '''
    def get_value_fields(self, **kwargs) -> dict:
        if 'schema_name' in kwargs: current_value_schema = self.get_latest_schema(kwargs.get('schema_name'))
        else: current_value_schema = kwargs.get('schema')
        value_fields = []
        all_fields = []
        required_value_fields = []
        optional_value_fields = []

    
        if current_value_schema is None:     # No schema
            value_dict = dict()
            value_dict['name'] = 'value'
            value_dict['type'] = ['string', 'float']
            value_fields = [value_dict]
            all_fields.append(value_dict)
            required_value_fields.append(value_dict)
            return {
                'value_fields': all_fields,
                'required_value_fields': required_value_fields,
                'optional_value_fields': optional_value_fields
            }

        if current_value_schema.schema_type == 'AVRO': # Avro schema
            value_fields = eval('dict('+current_value_schema.schema_str+')')['fields']
            
        elif current_value_schema.schema_type == 'JSON': # JSON schema

            json_list = []
            value_fields = eval('dict('+current_value_schema.schema_str+')')['properties']
            for value_field_name, value_field_contents in value_fields.items():
                my_dict = {}
                my_dict['name'] = value_field_name
                for k, v in value_field_contents.items():
                    if k == 'description': my_dict['doc'] = v
                    else: my_dict[k] = v
                    
                json_list.append(my_dict)
            value_fields = json_list
        
        for value_field in value_fields:
            
            required = True
            nested = False
            if isinstance(value_field['type'], list): # the type is a list
                if 'null' in value_field['type']: required = False # check for null
                # print (value_field['type'])    
                for type_ in value_field['type']: # iterate the type
                    nested = False
                    # Check for nested elements
                    if isinstance(type_, dict): # Dictionary of elements. 
                        nested = True # toggle nested for the purposes of adding the fields
                        if 'type' in type_.keys(): # type exists in dictionary
                            
                            if type_['type'] == 'record': # type is array. Get the fields from the items key
                                for element in type_['fields']: # iterate the sub elements
                                    subtype_required = True # toggle on required for subtype
                                    element['record'] = type_['name'] # add the parent
                                    element['parent'] = value_field['name']
                                    # Add the field to all fields
                                    all_fields.append(element)
                                    # Add the field to either required or optional
                                    if isinstance(element['type'], list):
                                        
                                        for subtype_ in element['type']: # iterate type that is list
                                            if subtype_ == 'null': subtype_required = False
                                    else:
                                        if element['type'] == 'null': subtype_required = False
                                    # Set if optional or required
                                    if subtype_required and required: required_value_fields.append(element)
                                    else: optional_value_fields.append(element)
                        else: # the type is a list. there is a dictionary in the type. type does not exist in in the keys
                            pass
                    else: # the type is a list. there is a dictionary in the type
                        pass

            else: # the type is not a list. It is not nested
                if value_field == 'null': required = False
            
            if not nested:
                all_fields.append(value_field)
                if required: required_value_fields.append(value_field)
                else: optional_value_fields.append(value_field)
    
        return {
            'value_fields': all_fields,
            'required_value_fields': required_value_fields,
            'optional_value_fields': optional_value_fields
        }

    # END GET VALUE FIELDS //////////////////////////////////////////////////////////////////////////////////////////////


    # GET KEY FIELDS ////////////////////////////////////////////////////////////////////////////////////////////////////

    def get_key_fields(self, **kwargs) -> dict:
        if 'schema_name' in kwargs: current_key_schema = self.get_latest_schema(kwargs.get('schema_name'))
        else: current_key_schema = kwargs.get('schema')

        
        # current_key_schema = schema
        key_fields = []
        all_fields = []
        required_key_fields = []
        optional_key_fields = []

        # Return value for no key
        if current_key_schema is None:
            key_dict = dict()
            key_dict['name'] = 'key'
            key_dict['type'] = ['null', 'string']
            key_fields = [key_dict]
            all_fields.append(key_dict)
            optional_key_fields.append(key_dict)
            return {
                'key_fields': all_fields,
                'required_key_fields': required_key_fields,
                'optional_key_fields': optional_key_fields
            }

        # process avro schema
        elif current_key_schema.schema_type == 'AVRO': 
            key_fields = eval('dict('+current_key_schema.schema_str+')')['fields']

        # process json schema
        elif current_key_schema.schema_type == 'JSON': 

            json_list = []
            key_fields = eval('dict('+current_key_schema.schema_str+')')['properties']
            for key_field_name, key_field_contents in key_fields.items():
                my_dict = {}
                my_dict['name'] = key_field_name
                for k, v in key_field_contents.items():
                    if k == 'description': my_dict['doc'] = v
                    else: my_dict[k] = v
                    
                json_list.append(my_dict)
            key_fields = json_list

        """
        The key fields have been processed according to the schema. 
        """
        all_fields = []

        for key_field in key_fields:

            all_fields.append(key_field)
            
            not_required = False
            if type(key_field['type']) is list:
                for type_ in key_field['type']:
                    if type_ == 'null': not_required = True
            else: 
                if key_field == 'null': not_required = True

            if not_required: optional_key_fields.append(key_field)
            else: required_key_fields.append(key_field)

        return {
            'key_fields': all_fields,
            'required_key_fields': required_key_fields,
            'optional_key_fields': optional_key_fields
        }

    # END GET KEY FIELDS //////////////////////////////////////////////////////////////////////////////////////////////

    def get_field_names(self, list_: list) -> list:
        
        if not isinstance(list_, list): return []
        my_list = list()
        for field in list_:
            
            my_list.append(field['name'])
        return my_list

    
    def schema_exists(self, schema_name: str) -> bool:
        try:
            value = self.get_latest_schema(schema_name)

            if value is None: return False
        except:
            return False

        return True
        

    def update_schema(self, schema_name: str,schema_dict: dict = None) -> bool:
        
        def format_string(d):
            
            formatted_schema_str = str(d).replace("'", '"') # replace single quotes with double quotes
            return formatted_schema_str

        if schema_dict.get('fields') is not None:
            # Check the field types before adding
            for field in schema_dict['fields']:
                for count, t in enumerate(field['type']):
                    if t not in avro_schema_field_types: 
                        try: field['type'].pop(count)
                        except: pass
                    
                    if len(field['type']) == 0:
                        field['type'].append('string')

            new_schema = Schema(schema_str=format_string(schema_dict),schema_type='AVRO')
        
        # Set JSON fields
        elif schema_dict.get('properties') is not None:
            for key, value in schema_dict['properties']:
                for count, t in enumerate(value['type']):
                    if t not in json_schema_field_types:
                        value['type'].pop(count)

                    if len(value['type']) == 0:
                        value['type'].append('string')
                
            new_schema = Schema(schema_str=format_string(schema_dict),schema_type='JSON')
        else: return False
        # Try to update the schema. On Failure, iterate through compatiblity modes and try to change
        
        try:
            # Register the schema
            print ('trying to register schema...')
            self.register_schema(schema_name, new_schema)
            return True
        except Exception as e:
            print (e)
            pass
            # print ('failed')
        
        for count, comp in enumerate(schema_compatibility_modes):
            try:
                self.set_compatibility(schema_name, level=comp)
                # Register the schema
                self.register_schema(schema_name, new_schema)
                return True
            except Exception as e:
                print (e)
                pass    
            
        return False

    # END DELETE FIELD ///////////////////////////////////////////////////////////////////////////

    def alter_field(self, schema_name: str, field_name: str, field_type: str, 
                        required: bool=True, notes: str='Field generated with kp_fraydit'
                    ) -> bool:
        '''
        schema name is passed instead of an actual schema. This is to ensure that the schema is not stale. So, each time
        this function is called, a fresh pull (which is slow) will result in the schema always being fresh.
        '''
        if not self.schema_exists: 
            print (f'SchemaEngine.alter_field: schema ({schema_name}) does not exist')
            return False
        
        # Pull fresh schema to ensure no updates have been applied instead of using a stale schema
        current_schema = self.get_latest_schema(schema_name)
        current_schema_dict = json.loads(current_schema.schema_str)

        if current_schema.schema_type == 'AVRO':        
            # Check if the field exists first and create it if not
            exists = False
            for count, field in enumerate(current_schema_dict['fields']): 
                if field['name'] == field_name:
                    exists = True
                    current_field = field
                    # delete the field
                    del current_schema_dict['fields'][count]
                    break
            
            if not exists: current_field = {'name': field_name}        
            
            self.delete_field(schema_name, field_name)

            current_field['type'] = field_type
            current_field['doc'] = notes
            
            # Add null for optional field
            if not required:
                if not isinstance(current_field['type'], list): # Create list from existing field
                    new_list = []
                    new_list.append(current_field['type'])
                    current_field['type'] = new_list
                
                # Add null to field type
                current_field['type'].append('null')
            # End not required /////////////////////////////
            
            # Validate field type
            if not isinstance(current_field['type'], list):
                if current_field['type'] not in avro_schema_field_types:
                    current_field['type'] = 'string'
            else:
                list_types = [t for t in current_field['type'] if t in avro_schema_field_types]
                current_field['type'] = list_types
                if len(list_types) > 1:
                    current_field['type'] = list_types
                    
                elif len(list_types) == 1:
                    current_field['type'] = list_types[0]
                else:
                    current_field['type'] = 'string'

            current_schema_dict['fields'].append(current_field)
            
            # Update the schema
            return self.update_schema(schema_dict=current_schema_dict, schema_name=schema_name)
        
        elif current_schema.schema_type == 'JSON':
            # Check if the field exists first and create it if not
            for field in current_schema_dict['properties']:
                if field == field_name:
                    current_field = current_schema_dict['properties'].get(field)
                    del current_schema_dict['properties'][field]        
                    break
                else:
                    current_field = {}

            # delete the field
            
            self.delete_field(schema_name, field_name)

            current_field['type'] = field_type
            current_field['description'] = notes
            
            # Add null for optional field
            if not required:
                if not isinstance(current_field['type'], list): # Create list from existing field
                    new_list = []
                    new_list.append(current_field['type'])
                    current_field['type'] = new_list
                
                # Add null to field type
                current_field['type'].append('null')
            # End not required /////////////////////////////
            
            # Validate field type
            if not isinstance(current_field['type'], list):
                if current_field['type'] not in json_schema_field_types:
                    current_field['type'] = 'string'
            else:
                list_types = [t for t in current_field['type'] if t in json_schema_field_types]
                current_field['type'] = list_types
                if len(list_types) > 1:
                    current_field['type'] = list_types
                    
                elif len(list_types) == 1:
                    current_field['type'] = list_types[0]
                else:
                    current_field['type'] = 'string'

            current_schema_dict['properties'][field_name] = current_field
            
            # Update the schema
            return self.update_schema(schema_dict=current_schema_dict, schema_name=schema_name)

        

    def delete_field(self, schema_name: str, field_name: str) -> bool:
        '''
        Schema will only delete a null field that was initialized as null.
        '''
        
        if not self.schema_exists: 
            print (f'SchemaEngine.alter_field: schema ({schema_name}) does not exist')
            return False
        
        
        current_schema = self.get_latest_schema(schema_name)
        current_schema_dict = json.loads(current_schema.schema_str)
    
        if current_schema.schema_type == 'AVRO':
            for count, field in enumerate(current_schema_dict['fields']): 
                if field['name'] == field_name:
                    # delete the field
                    del current_schema_dict['fields'][count]

            return self.update_schema(schema_dict=current_schema_dict, schema_name=schema_name)
        
        elif current_schema.schema_type == 'JSON':
            # delete the field
            if current_schema_dict['properties'].get(field_name):
                del current_schema_dict['properties'][field_name]

            return self.update_schema(current_schema_dict, schema_name, 'JSON')
