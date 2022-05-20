

class SubjectInvalidError(Exception):
    def __init__(self, message):
        self.message = message
        print(self)

    def __str__(self):
        print (f'SubjectInvalidError: {self.message}')
    
class ValidateError(Exception):
    def __init__(self, message) -> None:
        self.message = message
        print (self)
        
    def __str__(self):
        print (f'ValidateError: {self.message}')