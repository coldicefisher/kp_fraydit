class CustomError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
            if len(args) > 1: self.error_type = args[1]
        else:
            self.message = None

        print(self)
    
    def __str__(self):
    
        if hasattr(self, 'message') and hasattr(self, 'error_type'):
            return f'CustomError: {self.error_type}: {self.message}'
        elif hasattr(self, 'message'):
            return f'CustomError: {self.message}'
        else:
            return 'CustomError has been raised'
