"""
MetaClass is a class that inherits type. Type is called when an object is created. class MyObject is equivalent to
type(MyObject, bases, args) something like that.

Here a metaclass returns the instance that has already been created. This method is preferred over the plain ol new
dunder method returning the instance. In that method, properties do not behave correctly.

"""

class SingletonMeta(type):
    def __init__(cls, name, bases, attrs, **kwargs):
        super().__init__(name, bases, attrs)
        cls._instance = None

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
        
        return cls._instance

class SingletonSubject(metaclass=SingletonMeta):
    

    def __init__(self):
        
        # Put any initialization here.
        self.__observers = set()
        self.__subject_state = {}
        