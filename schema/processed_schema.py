import requests
import json

from confluent_kafka.schema_registry import Schema

from kp_fraydit.schema.fields import Field, Fields
from kp_fraydit.schema.schema_client import SchemaEngine
from kp_fraydit.schema.schema_errors import SubjectInvalidError, ValidateError
from kp_fraydit.connections.connection import KafkaConnection

kConn = KafkaConnection()
eng = SchemaEngine()

from pathlib import Path
import os

'''
todo create error classes to handle data types
'''
class ProcessedSchema:
    def __init__(self, schema: Schema, subject_name: str) -> None:
        self.__subject_name = subject_name
        self.debug_on = False
        self.__raw_schema = schema
        self.__fields = None
        self.__head = dict()
        self.__process_schema()


    @classmethod
    def from_topic_name(cls, topic_name: str, is_value_schema: bool = True):
        if is_value_schema == True: subject_name = f'{topic_name}-value'
        else: subject_name = f'{topic_name}-key'
                
        return cls(eng.get_latest_schema(subject_name), subject_name)


    @classmethod
    def from_subject_name(cls, subject_name: str) -> object:
        fetched_schema = eng.get_latest_schema(subject_name)
        schema_type = subject_name.split('-')[-1:]
        if schema_type == 'value': is_value_schema = True
        elif schema_type == 'key': is_value_schema = False
        else: is_value_schema = True
        '''
        self.__raw_schema will be None if the schema does not exist. This should be handled in the processor method
        '''
        return cls(fetched_schema, subject_name)


    @classmethod
    def from_raw(cls, schema_string: str, schema_type: str, subject_name: str) -> object:
        created_schema = Schema(str(schema_string), schema_type)
        return cls(created_schema, subject_name)


    def debug(self, msg):
        if self.debug_on: print(msg)

    @property
    def raw_schema(self) -> Schema:
        return self.__raw_schema

    
    @property
    def is_value_schema(self) -> bool:
        
        # if self.__subject_name is not None or not self.__subject_name:
        if self.subject_name is not None:
            if self.__subject_name[-5:] == 'value': return True
            if self.__subject_name[-3:] == 'key': return False
        else:
            return False


    @property
    def subject_name(self) -> str:
        # if not SchemaEngine().schema_exists(self.__subject_name): return None
        # if not self.__subject_name[-6:] == '-value' and not self.__subject_name[-4:] == '-key': return None
        return self.__subject_name

    
    @subject_name.setter
    def subject_name(self, value: str) -> None:
        
        if SchemaEngine().schema_exists(value): 
            raise SubjectInvalidError(f'Subject name {value} does not exist')
            
        # Check for bad subject name format
        if not value[-6:] == '-value' and not value[-4:] == '-key':
            raise SubjectInvalidError(f'Subject name {value} cannot be processed. Please make sure -value or -key is at the end of the subject name')
        
        
        self.__subject_name = value


    @property
    def schema_type(self) -> str:
        try:
            return self.__raw_schema.schema_type
        except:
            return None


    @property
    def head(self) -> list:
        return self.__head

    @property
    def fields(self) -> Fields:
        return self.__fields
    
    @property
    def schema_str(self) -> str:
        if self.raw_schema is None: return ''
        return self.raw_schema.schema_str

    @property
    def schema_dict(self) -> dict:
        return eval('dict('+self.schema_str+')')
    # PROCESS SCHEMA ///////////////////////////////////////////////////////////////////////////////////////////////
    # //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    def __process_schema(self) -> dict:
        
        value_fields = []
        all_fields = []
        required_fields = []
        optional_fields = []

        '''
            NO SCHEMA methods. 
            get whether it is a value or a key schema. Use that to build for a no schema type and return fields
        '''

        # no schema (raw schema applies to None if it is pulled If one is passed, no worries). 
        # and no type specified. Return value schema
    
        def return_value_schema(): # PRIVATE FUNCTION OF PROCESS SCHEMA

            fields_ = Fields()
            name = 'value'
            types = ['string', 'float'] # set to all value types
            desc = ""
            fields_.append(Field(name, types, desc))
            
            self.__fields = fields_
        
            return True


        def return_key_schema(): # PRIVATE FUNCTION OF PROCESS SCHEMA

            fields_ = Fields()
            name = 'key'
            types = ['string', 'float', 'null'] # set to all value types
            desc = ""
            fields_.append(Field(name, types, desc))
            
            self.__fields = fields_
        
            return True


        if self.raw_schema is None and self.is_value_schema is None: 
            return return_value_schema()

        if self.raw_schema is None and self.is_value_schema is True: 
            return return_value_schema()

        if self.raw_schema is None and self.is_value_schema is False:
            return return_key_schema()
        
        

        '''
        Process the head properties
        '''
        schema_dict = self.schema_dict
        
        for key, value in schema_dict.items():
            if not key == 'fields' or not key == 'properties': self.__head[key] = value

        '''
        Process the schema fields by schema type. all_fields uses eval to build a list of dictionaries that represent the
        fields
        '''

        if self.raw_schema.schema_type == 'AVRO': # Avro schema
            all_fields = schema_dict['fields']

        elif self.raw_schema.schema_type == 'JSON': # JSON schema
            json_list = []
            value_fields = schema_dict['properties']
            for value_field_name, value_field_contents in value_fields.items():
                my_dict = {}
                my_dict['name'] = value_field_name
                for k, v in value_field_contents.items():
                    if k == 'description': my_dict['doc'] = v
                    else: my_dict[k] = v
                    
                json_list.append(my_dict)
            all_fields = json_list
        
        
        fields_ = Fields()

        for value_field in all_fields:
            name = value_field['name']
            my_type = value_field['type']
            try: desc = value_field.get('doc')
            except: desc = ''
            
            '''
            1.1 iterate the field types. 1.2 If any type is a dictionary, then the record is a nested type. Ex:
            type: [
                'null',
                {                                                 1.2 will test true here
                    'name': 'TestPathElement',
                    'type': 'record',                             1.3 will test true here 1.4 will test true for 'record'
                    'fields': [
                        {                                  1.5 will iterate these elements
                            'name': 'sourceNested',
                            'type': ['string', 'float']
                        }
                    ]
                }
                
            ]
            nested stores the value of whether the rcord is nested or not. At the end of the loops, all non-nested fields
            are added and this boolean controls that. Both record and parent is set to none at the beginning of the iteration
            
            1.3 Check if the nested field has a 'type' key. 1.5 Iterate the fields of the nested element and build a field
            '''
            for type_ in value_field['type']: # 1.1 Iterate the field types.
                nested = False
                record = None
                parent = None            

                if isinstance(type_, dict): # 1.2 The field type contains a record
                    nested = True # toggle nested for the purposes of adding the non-nested fields
                    if 'type' in type_.keys(): # 1.3 Ensure the that 'type' is in the keys of the nested field. If not, we are not going to evaluate
                        if type_['type'] == 'record': # 1.4 type is record
                            for element in type_['fields']: # 1.5 iterate the sub elements
                                record = type_['name']
                                parent = value_field['name']
                                name = element['name']
                                my_type = element['type']
                                if isinstance(my_type, list): list_type = my_type
                                else:
                                    list_type = []
                                    list_type.append(my_type)
                                if 'null' in value_field['type']: list_type.append('null')
                                try: desc = value_field.get('doc')
                                except: desc = ''
                                fields_.append(Field(name, my_type, desc, record, parent))
                                continue
                        else: # the type is a dict. there is a dictionary in the type. type does not exist in in the keys
                            pass
                    else: # the type is a dict. there is a dictionary in the type
                        pass

                else: # the type is not a dict. It is not nested
                    pass
            
            if not nested: fields_.append(Field(name, my_type, desc, record, parent))
        
        self.__fields = fields_
        return True
        

    # END PROCESS SCHEMA ///////////////////////////////////////////////////////////////////////////////////////////
    # //////////////////////////////////////////////////////////////////////////////////////////////////////////////
            

    def validate(self, validation_schema_source: str = None, 
                additional_fields: Fields = None, name: str = None, namespace: str = None, desc: str = None):
        def validate_update(head, fields):
            self.debug(f'Updating the schema')
            
            # Set the head namespace property
            if head.get('namespace') is not None:
                head['namespace'] = f'{head["namespace"]}.{self.subject_name}'
            
            schema_dict = head
            schema_dict.update(fields)
            self.debug (f'Schema dict: {schema_dict} Type: {type(schema_dict)} Subject name: {self.subject_name}')
            self.debug
            try:
                update_success = SchemaEngine().update_schema(schema_dict=schema_dict, schema_name=self.subject_name)
                self.debug (f'Update status: {update_success}')
            except Exception as e:
                print (e)


        
        format = None # This sets the format that we will use
        fields_to_add = Fields()

        '''
        Something must be passed to validate against. There was no validation_schema_source and: 
        no additional_fields, name, and namespace provided.
        '''
        if validation_schema_source is None: 
            self.debug (f'validation_schema_source is None. Checking if the necessary arguments have been passed')
            self.debug (f'additional_fields: {additional_fields}')
            self.debug (f'name: {name}')
            self.debug (f'namespace: {namespace}')
            if additional_fields is None and name is None and namespace is None: 
                raise ValidateError(f'Incorrent validation parameters provided. You must either supply:\nvalidation_schema_source: {validation_schema_source}\nor supply all:\nadditional_fields: {additional_fields}\nname: {name}\nnamespace: {namespace}')
            validation_schema_source = 'base_schema_value.avro'
        
        # Load the json data structure if a validation schema is provided
        if validation_schema_source is not None:
            self.debug (f'validation_schema_source is not None: {validation_schema_source}')
            if validation_schema_source[:4] == 'http':
                try:
                    self.debug (f'validation schema is url')
                    r = requests.get(validation_schema_source)
                except:
                    raise ValidateError(f'Could not load the url: {validation_schema_source}\nCheck to see if the file exists.')        
                try:
                    data = r.json()
                except:
                    raise ValidateError(f'Could not load the json from url: {validation_schema_source}\nYour json format is incorrect. Try copying the json directly to a schema in the control panel for confluent kafka and see what your error is.')
                
            else:
                self.debug (f'validation schema must be a file')
                script_path = os.path.abspath(__file__)
                script_dir = os.path.split(script_path)[0]
                abs_file_path = os.path.join(script_dir, validation_schema_source)
                with open(abs_file_path) as json_file:
                    try:
                        data = json.load(json_file)
                        self.debug (f'validation schema successfully loaded from local storage')
                    except:
                        raise ValidateError(f'Could not load the url or the file: {validation_schema_source}\nCheck to see if the url is correct if using a url. Check to see if the file exists when using a file on local storage')

        '''
        Check if there is data. This will pass True if there is a schema present. 
        Check the schema dictionary for a name or a title. The schema must be passed with a head or this fails. 
        If the format fails, then the routine checks for the necessary properties to build a schema from scratch.
        If building from scratch, the format is set to AVRO because we only build AVRO schemas from scratch.
        An error is raised if no schema provided AND no properties provided to build the schema.
        '''
    
        if data:
            self.debug (f'There is data loaded from the validation schema')
            if data.get('title') is not None:
                self.debug (f'There is a title attribute in the head. Setting JSON schema')
                validation_schema_format = ProcessedSchema.from_raw(data, 'JSON', self.subject_name)
                validation_schema = ProcessedSchema.from_raw(data, validation_schema_format, self.subject_name)
                format = 'JSON'
            elif data.get('name') is not None:
                self.debug (f'There is a name attribute in the head. Setting AVRO schema')
                validation_schema_format = ProcessedSchema.from_raw(data, 'AVRO', self.subject_name)
                validation_schema = ProcessedSchema.from_raw(data, validation_schema_format, self.subject_name)
                format = 'AVRO'
    
        '''
        At this point, we have everything to build a head. Either there is a schema or there are arguments to build from scratch.
        '''
    
        my_head = {}
        '''
        We have an empty head: my_head = dict(). Then we read if there is a validation schema provided. Then we check
        if there is a schema on self if no validation schema is provided. We set it equal to its own head.
        We have no head if either a validation or self schema is not provided/present. 
        We test if the head has: name or title. We can assume here that those values are always required in an AVRO/JSON
        schema respectively. If not, it is formatted wrong and invalid or it is missing altogether.
        name = AVRO
        title = JSON
        If the schema does not have these
        '''

        # Set the head if a validation schema has been provided and default my_fields to the validation schema
        if validation_schema.raw_schema is not None:
            my_head = validation_schema.head
            
        # Set the head to self.head if there is a self schema and there is no validation schema
        elif self.raw_schema is not None and validation_schema.raw_schema is None:
            my_head = self.head

        
        # Override an existing head with the user specified properties if provided
        # Set AVRO head settings if the current head is AVRO
        if my_head.get('name') is not None: 
            if name is not None: my_head['name'] = name
            if namespace is not None: my_head['namespace'] = namespace
            if desc is not None: my_head['doc'] = desc
            
            # check for a namespace.
            if my_head.get('namespace') is None: 
                raise ValidateError(f'The head: {my_head}, is invalid. The schema is deemed "AVRO". In case there is no validation schema provided to the validate function to set the head, then the classes head will be set. If neither conditions are True, then you must pass a name and a namespace argument to the validate function to build the head. There is a head name, but there is no namespace name. You need to pass a namespace argument. \nFunction arguments:\nname: {name}\nnamespace: {namespace}\ncurrent head: {my_head}')
            format = 'AVRO'
        
        # Set JSON head settings if the current head is JSON
        elif my_head.get('title') is not None: 
            if name is not None: my_head['title'] = name
            if namespace is not None: my_head['$schema'] = namespace
            if desc is not None: my_head['description'] = desc
            
            # Check for a schema
            if my_head.get('$schema') is None: raise ValidateError(f'The head: {my_head}, is invalid. The schema is deemed "JSON". In case there is no validation schema provided to the validate function to set the head, then the classes head will be set. If neither conditions are True, then you must pass a name and a namespace argument to the validate function to build the head. There is a head name, but there is no namespace name. You need to pass a namespace argument. \nFunction arguments:\nname: {name}\nnamespace: {namespace}\ncurrent head: {my_head}')
            format = 'JSON'

        '''
        Check to see if the current head is invalid.
        If the head is invalid, check to see if the minimum number of arguments are provided to build a head.
        If so, build the head.
        Return error if there is not enough arguments to build a head if the head is invalid
        '''
        
        
        if name is not None: my_head['name'] = name
        if namespace is not None: my_head['namespace'] = namespace 
        # Only support avro if no head is provided. Must have minimum: name and namespace to build a head
        if desc is not None: my_head['doc'] = desc
        if format == 'AVRO': my_head["type"] == "record"
        else: my_head['type'] = 'object'
        
        # raise ValidateError(f'The head: {my_head}, is invalid. The correct arguments (name and namespace) were not provided. In case there is no validation schema provided to the validate function to set the head, then the classes head will be set. If neither conditions are True, then you must pass a name and a namespace argument to the validate function to build the head. \nFunction arguments:\nname: {name}\nnamespace: {namespace}\ncurrent head: {my_head}')

        # Check to see if the format is correct
        # if format is None: raise ValidateError(f'The head did not set a format property. This is a bug. Fix it\nhead: {head}')    
        
        # END BUILD THE HEAD //////////////////////////////////////////////////////////////////////////////
        # /////////////////////////////////////////////////////////////////////////////////////////////////


        # ADD THE FIELDS //////////////////////////////////////////////////////////////////////////////////
        # /////////////////////////////////////////////////////////////////////////////////////////////////
        
        self.debug (f'Set head: {my_head}')
        self.debug (f'The head is set')
        
        

        '''
        Check to see if a validation schema was provided. 
        
        Note:We do this by checking if the validation schema and the
        classes" schema are identical. This returns True because we set them equal if the validation schema was not
        provided and the classes" schema was present.
        
        If a validation schema was provided, we validate the fields. We test each field in the validation_schema against
        the classes" schema. We know they are different as described in the notes.
        '''
        
        self.debug(f'self schema: {self.schema_str}')
        self.debug(f'validation schema: {validation_schema.schema_str}')
        if not validation_schema.schema_str == self.schema_str:
            self.debug (f'Setting fields from the validation schema')
            self.debug(f'validation schema fields: {validation_schema.fields}')
            if format == 'JSON': v = 'properties'
            else: v = 'fields'
            for field in data[v]:
                # Field is missing. Add it to the collection
                self.debug(f'field: {field["name"]}')
                if field['name'] not in self.fields.names: 
                    self.debug(f'Appending missing field: {field["name"]}')
                    if format == 'JSON': 
                        
                        for key, value in field['properties']:
                            my_name = key
                            for v in value:
                                my_type = v['type']
                                my_desc = v['description']
                        fields_to_add.append(Field(my_name, my_type, my_desc))
                    else:
                        fields_to_add.append(Field(field['name'], field['type'], field['doc']))
                    
                elif field['name'] in self.fields.names:
                    '''
                    Field is not missing. Check the types. If they are not equal,append the field to the colleciton
                    '''
                    
                    if format == 'AVRO': 
                        field_types = field['type']
                        field_desc = field['doc']
                    else: 
                        field_types = field['name']['type']
                        field_desc = field['name']['description']

                    self.debug (f'Field is not missing. Checking types {field_types} against {self.fields[field["name"]].types}')
                    
                    if not sorted(field_types) == sorted(self.fields[field['name']].types): 
                        self.debug (f'Appending incorrect field types: {field["name"]}')
                        fields_to_add.append(Field(field['name'], field_types, field_desc))

        '''
        Check to see if there are additional fields and that they are not empty and that a list was in fact passed.
        Iterate the additional fields. If the additional field is not in the collection, add it.
        '''

        # self.debug(f'additional fields: {additional_fields.names}')
        if additional_fields is not None and isinstance(additional_fields, Fields) and len(additional_fields) > 0:
            self.debug(f'adding additional fields')
            for field in additional_fields:
                if field.name not in fields_to_add.names:
                    self.debug(f'field: {field.name}')
                    fields_to_add.append(field)
                    '''
                    Check to see if the field to add and the current schemas fields are equal. If they are, test the
                    types. If the current schema has null, it is safe to update the field.
                    Add the field
                    '''
                    if field.name in self.fields.names:
                        if 'null' in self.fields[field.name].types:
                            fields_to_add.append(field)
        

        '''
        Add the original schemas fields that were not added in the validation routine above
        '''
        for field in self.fields:
            if field.name not in fields_to_add.names:
                if not field.name == 'value' and not field.name == 'key':
                    fields_to_add.append(field)

        self.debug ('The fields are set')
        # for field in fields_to_add:
        #     self.debug (f'Set field: {field.name}, {field.types}, {field.desc}')
            
        self.debug (f'fields: {fields_to_add.names}')

        '''
        Check each field to see if there is a need to validate
        '''
        needs_update = False
        self.debug(f'Current schema fields: {self.fields.names}')
        
        for field in fields_to_add:
            self.debug(field.name)
            self.debug('iterating')
            if field.name not in self.fields.names: 
                needs_update = True
                break
            else:
                if not sorted(field.types) == sorted(self.fields[field.name].types): 
                    needs_update = True
                    break
        
        self.debug(f'Needs update: {needs_update}')
        # Update the schemas
        if needs_update:
            if format == 'AVRO':
                validate_update(validation_schema.head, fields_to_add.to_dict('AVRO'))
            elif format == 'JSON':
                validate_update(validation_schema.head, fields_to_add.to_dict('JSON'))
            
