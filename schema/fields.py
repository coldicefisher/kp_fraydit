from kp_fraydit.class_iterators import ClassIterator


class Field:
    def __init__(self, name: str, types: list, desc: str = '', record: str = None, parent: str = None) -> None:
        self.__name = name
        self.__types = types
        self.__desc = desc
        self.__record = record
        self.__parent = parent

    def __str__(self) -> str:
        l = []
        l.append('\n')
        l.append(f'#################################\n')
        l.append(f'name: {self.name}')
        l.append(f'types: {" ".join(self.types)}')
        l.append(f'desc: {self.desc}')
        if self.record is not None: l.append(f'record: {self.record}')
        else: l.append('record: ')
        if self.parent is not None: l.append(f'parent: {self.parent}')
        else: l.append('parent: ')
        l.append(f'\n################################# \n')
        return '\n'.join(l)
        


    @property
    def name(self):
        return self.__name


    @property
    def types(self):
        my_types = []
        if not isinstance(self.__types, list): my_types.append(self.__types)
        else: my_types = self.__types
        if my_types is None: return []
        return my_types

    @property
    def desc(self):
        if self.__desc is None: return ''
        return self.__desc

    @property
    def record(self):
        return self.__record

    @property
    def parent(self):
        return self.__parent

    def to_dict(self, schema_type: str = 'AVRO') -> dict:
        if schema_type == 'AVRO':
            d = {}
            d['doc'] = self.desc
            d['name'] = self.name
            d['type'] = self.types
            return d
        elif schema_type == 'JSON':
            d = {}
            d[self.name] = {}
            d[self.name]['description'] = self.desc
            d[self.name]['type'] = self.types
            return d
        else: return {}

class Fields(ClassIterator):
    def __init__(self, group_list=None) -> None:
        super().__init__(group_list)

    def __getitem__(self,key) -> object:
        for group in self.objList:
            if group.name == key: return Field(group.name, group.types, group.desc, group.record, group.parent)

    @property
    def required(self):
        l = Fields()
        for field in self.objList:
            if 'null' not in field.types:
                l.append(field)
        return l


    @property
    def optional(self):
        l = Fields()
        for field in self.objList:
            if 'null' in field.types:
                l.append(field)
        return l

    @property
    def names(self) -> list:
        return [field.name for field in self.objList]
    
    def to_dict(self, schema_type: str = 'AVRO') -> dict:
        
        if schema_type == 'AVRO':
            d = {}
            d['fields'] = []
            for field in self.objList:
                d['fields'].append(field.to_dict())
            return d
        
        elif schema_type == 'JSON':
            d = {}
            d['properties'] = {}
            for field in self.objList:
                d['properties'][field.name] = {}
                d['properties'][field.name]['description'] = field.desc
                d['properties'][field.name]['type'] = field.types
            return d
        else:
            return {}