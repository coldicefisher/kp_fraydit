from typing import Dict
from kafka_python.connections.connection_settings import ConnectionSettings


"""
Define a one-to-many dependency between objects so that when one object
changes state, all its dependents are notified and updatedautomatically.
"""

import abc


class Subject:
    """
    Know its observers. Any number of Observer objects may observe a
    subject.
    Send a notification to its observers when its state changes.
    """

    def __init__(self):
        self._observers = set()
        self._subject_state = {}
        self._subject_state['broker_online'] = False

    def attach(self, observer):
        observer._subject = self
        self._observers.add(observer)
        print ('Attached observer')

    def detach(self, observer):
        observer._subject = None
        self._observers.discard(observer)
        print ('Detached observer')

    def _notify(self):
        for observer in self._observers:
            observer.update(self._subject_state)
            print ('notifying')

    @property
    def subject_state(self):
        return self._subject_state

    @subject_state.setter
    def subject_state(self, arg):
        if not isinstance(arg, dict): 
            print ('returning')
            return
        self._subject_state = arg
        self._notify()
        
    @property
    def broker_online(self):
        return self._subject_state['broker_online']

    @broker_online.setter
    def broker_online(self, value):
        self._subject_state['broker_online'] = value
        print ('set broker')
        self._notify()

    
class Observer(metaclass=abc.ABCMeta):
    """
    Define an updating interface for objects that should be notified of
    changes in a subject.
    """

    def __init__(self):
        self._subject = None
        self._observer_state = None

    @abc.abstractmethod
    def update(self, arg):
        pass


class ConcreteObserver(Observer):
    """
    Implement the Observer updating interface to keep its state
    consistent with the subject's.
    Store state that should stay consistent with the subject's.
    """

    def update(self, arg):
        print (arg)
        self._observer_state = arg
        



