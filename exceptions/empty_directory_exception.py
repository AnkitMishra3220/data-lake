class EmptyDirectoryException(Exception):
    def __init__(self, dir_path, message="Empty Directory :"):
        self.message = message + dir_path
        super().__init__(self.message)