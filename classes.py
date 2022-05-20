
class BaseClass:
    def __repr__(self) -> str:
        all_obj_list = []
        str_list = [f'{k}: {v}' for k, v in self.__dict__.items()]
        all_obj_list.append('          '.join(str_list))

        return '          '.join(all_obj_list)

    def __str__(self) -> str:
        all_obj_list = []
        all_obj_list.append('\n')
        all_obj_list.append(f'#################################\n')
        str_list = [f'{k}: {v}' for k, v in self.__dict__.items()]
        all_obj_list.append('\n'.join(str_list))
        all_obj_list.append(f'\n ################################# \n')

        return '\n'.join(all_obj_list)

