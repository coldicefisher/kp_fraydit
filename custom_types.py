import typing

def flatten_list(list_):
    list_of_lists = []
    flattened_list = []
    list_of_lists.append(list_)
    
    while len(list_of_lists) > 0:
        for my_list in list_of_lists:
            for item in my_list:
                if isinstance(item, list):
                    list_of_lists.append(item)
                else:
                    flattened_list.append(item)
            
            list_of_lists.remove(my_list)
    return flattened_list

def flatten_dict(dict_: dict):
    dict_of_dicts = {}
    flattened_dict = {}
    dict_of_dicts = dict_
    
    
    while len(dict_of_dicts) > 0:
        append_items = {}
        added_dict = []
        for key, value in dict_of_dicts.items():
            if isinstance(value, dict):
                for key2, value2 in value.items():
                    append_items[key2] = value2
            else:
                flattened_dict[key] = value
            
            added_dict.append(key)
            
        for item in added_dict: # delete items that were added
            del dict_of_dicts[item]

        for key, value in append_items.items(): # add subitems
            dict_of_dicts[key] = value
            
    return flattened_dict



def key_exists(dict_to_check,key_to_check):
    if key_to_check in dict_to_check: return True
    return False

def check_int(s):
    s = str(s)
    if s[0] in ('-', '+'):
        return s[1:].isdigit()
    return s.isdigit()    