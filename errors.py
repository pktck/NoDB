class DatabaseAlreadyExists(Exception):
    pass

class DatabaseDoesNotExist(Exception):
    pass

class TableAlreadyExists(Exception):
    pass

class TableDoesNotExist(Exception):
    pass

class RowAlreadyExists(Exception):
    pass

class RowDoesNotExist(Exception):
    pass

class WriteOnReadOnlyRow(Exception):
    pass
