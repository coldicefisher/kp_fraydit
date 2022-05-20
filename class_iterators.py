from typing import ItemsView

class ClassIterator:
    
    def __init__(self, group_list=None, primary_key: str = "id") -> None:
        self._index = 0
        self.__primary_key = primary_key
        try:
            if group_list is None: self.__group_list = []
            else: self.__group_list = group_list
        except:
            self.__group_list = []
        
        

    def __str__(self) -> str:
        all_obj_list = []
        all_obj_list.append('\n')
        all_obj_list.append(f'#################################\n')
        for count, item in enumerate(self.__group_list):
            all_obj_list.append(f'Item: {count + 1} \n')
            str_list = [f'{k}: {v}' for k, v in item.__dict__.items()]
            all_obj_list.append('\n'.join(str_list))
            all_obj_list.append(f'\n################################# \n')

        return '\n'.join(all_obj_list)


    def __repr__(self) -> str:
        all_obj_list = []
        count_actual = 0
        all_obj_list.append('Item properties:          ')
        for count, item in enumerate(self.__group_list):
            
            all_obj_list.append(f'Number of regular attributes: {len(item.__dict__)}')
            if count == 0: all_obj_list.append(f'List of regular attributes {item.__dict__}')
            all_obj_list.append(f'Full list of attributes: {dir(item)}')
            count_actual = count

        all_obj_list.insert(0, f'Number of Items: {count_actual}')
        return '          '.join(all_obj_list)


    def __next__(self) -> object:
        if self._index < len(self.__group_list):
            result = self.__group_list[self._index]
            self._index += 1
            return result
            
        #end of iteration
        self._index = 0
        raise StopIteration
    
    def __iter__(self) -> object:
        return self

    def __len__(self) -> object:
        return len(self.__group_list)

    def __getitem__(self, key) -> object:
        try:
            for group in self.__group_list:
                v = getattr(group, self.__primary_key)
                if str(getattr(group, self.__primary_key)) == str(key): return group
        except:
            return None
            raise(KeyError('invalid key'))

    def __contains__(self, key) -> bool:
        for item in self.__group_list:
            if getattr(item, self.__primary_key) == key: return True
        return False
        
    def reset_iterator(self):
        self._index = 0


    def append(self, group) -> None:
        self.__group_list.append(group)

    def exists(self, key) -> bool:
        if self[key] is None: return False
        return True

    @property
    def objList(self) -> list:
        return self.__group_list