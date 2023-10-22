class ReaderNotFoundException(Exception):
    def __init__(self, file_type, message="Unknown File Format :"):
        self.message = message + file_type
        super().__init__(self.message)
