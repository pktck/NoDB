import os
import shutil
import errno
from helper_classes import Database
import errors

class Manager(object):
    def __init__(self, data_dir):
        self._data_dir = data_dir

    def getDatabase(self, db):
        return Database(self._data_dir, db)

    def createDatabase(self, db):
        try:
            os.mkdir(os.path.join(self._data_dir, db))
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise errors.DatabaseAlreadyExists(db)
            else:
                raise

    def removeDatabase(self, db):
        try:
            shutil.rmtree(os.path.join(self._data_dir, db))
        except OSError:
            if e.errno == errno.ENOENT: # if the table doesn't exist
                raise errors.DatabaseDoesNotExist(db)
            else:
                raise


