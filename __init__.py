import json
import datetime
import pickle
import fcntl
import os
import errno
import shutil
import types
import pprint


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


class LockBase(object):
    def __init__(self, fd):
        self._fd = fd

    def releaseLock(self):
        fcntl.flock(self._fd, fcntl.LOCK_UN)

    def __enter__(self):
        self.acquireLock()

    def __exit__(self, type, value, traceback):
        self.releaseLock()


class SharedLock(LockBase):
    def acquireLock(self):
        fcntl.flock(self._fd, fcntl.LOCK_SH)


class ExclusiveLock(LockBase):
    def acquireLock(self):
        fcntl.flock(self._fd, fcntl.LOCK_EX)


class NoDBBase(object):
    def getSharedLock(self):
        return SharedLock(self._fd_lock)

    def getExclusiveLock(self):
        return ExclusiveLock(self._fd_lock)

    def releaseLock(self):
        LockBase(self._fd_lock).releaseLock()


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
                raise DatabaseAlreadyExists
            else:
                raise

    def removeDatabase(self, db):
        try:
            shutil.rmtree(os.path.join(self._data_dir, db))
        except OSError:
            if e.errno == errno.ENOENT: # if the table doesn't exist
                raise DatabaseDoesNotExist
            else:
                raise


class Database(NoDBBase):
    def __init__(self, data_dir, db):
        self._data_dir = data_dir
        self._db = db
        self._fd_lock = open(os.path.join(self._data_dir, self._db, '.lock'), 'w')

    def getTable(self, table):
        return Table(self._data_dir, self._db, table)

    def createTable(self, table):
        try:
            os.mkdir(os.path.join(self._data_dir, self._db, table))
        except OSError as e:
            if e.errno == errno.EEXIST:
                raise TableAlreadyExists
            else:
                raise

    def removeTable(self, table):
        try:
            shutil.rmtree(os.path.join(self._data_dir, self._db, table))
        except OSError:
            if e.errno == errno.ENOENT: # if the table doesn't exist
                raise TableDoesNotExist
            else:
                raise


class Table(NoDBBase):
    def __init__(self, data_dir, db, table):
        self._data_dir = data_dir
        self._db = db
        self._table = table
        self._fd_lock = open(os.path.join(self._data_dir, self._db, self._table, '.lock'), 'w')

    def get(self, key):
        return Row(self._data_dir, self._db, self._table, key)

    def getWithLock(self, key):
        # user must manually release lock by calling releaseLock() on the row
        return Row(self._data_dir, self._db, self._table, key, is_locked=True)

    def create(self, key):
        with self.getExclusiveLock():
            row = Row(self._data_dir, self._db, self._table, key, is_new=True)
        return row

    def remove(self, key):
        try:
            os.remove(os.path.join(self._data_dir, self._db, self._table, key))
        except IOError as e:
            if e.errno == errno.ENOENT: # if the file doesn't exist
                raise RowDoesNotExist
            else:
                raise


class Row(NoDBBase):
    def __init__(self, data_dir, db, table, key, is_new=False, is_locked=False):
        self._data_dir = data_dir
        self._db = db
        self._table = table
        self._key = key
        self._filename = os.path.join(self._data_dir, self._db, self._table, key)

        if is_new: # note: must be run inside a table lock
            if os.path.exists(self._filename):
                raise RowAlreadyExists
            open(self._filename, 'w').close() # touch the file so we can lock it later

        try:
            self._fd_readonly = self._fd_lock = open(self._filename, 'r')
        except IOError as e:
            if e.errno == errno.ENOENT: # if the file doesn't exist
                raise RowDoesNotExist
            else:
                raise

        if not is_new:
            if is_locked:
                self.getExclusiveLock().acquireLock()
            self._loadContents()

    def _loadContents(self):
        self._fd_readonly.seek(0)
        with self.getSharedLock():
            contents = self._fd_readonly.read()
        contents = self._desearialize(contents)
        self.__dict__.update(contents)

    def _serializeHelper(self, d):
        if type(d) in (list, tuple):
            return map(self._serializeHelper, d)
        elif type(d) == dict:
            for key in d:
                d[key] = self._serializeHelper(d[key])
            return d
        elif type(d) == datetime.datetime:
            return {
                    '_NoDBSpecialType': 'datetime',
                    'value': d.ctime()}
        elif type(d) in (dict, list, tuple, str, unicode, int, long, float, bool, types.NoneType): # json-supported data types
            return d
        else:
            return {
                    '_NoDBSpecialType': 'pickled_object',
                    'value': pickle.dumps(d)}

    def __repr__(self):
        attribs = self._getPublicAttribs()
        return '<NoDB.Row object - key: %s>\n\n%s' % (self._key, pprint.pformat(attribs))

    def _getPublicAttribs(self):
        return dict([(key, value) for key, value in self.__dict__.items() if key[0] != '_'])

    def _serialize(self):
        attribs = self._getPublicAttribs()
        attribs = self._serializeHelper(attribs)
        attribs = json.dumps(attribs)
        return attribs

    def _desearializeHelper(self, d):
        if type(d) in (list, tuple):
            return map(self._desearializeHelper, d)
        elif type(d) == dict:
            if d.has_key('_NoDBSpecialType'):
                if d['_NoDBSpecialType'] == 'datetime':
                    return datetime.datetime.strptime(d['value'], '%a %b %d %H:%M:%S %Y')
                elif d['_NoDBSpecialType'] == 'pickled_object':
                    return pickle.loads(str(d['value']))
            else:
                for key in d:
                    d[key] = self._desearializeHelper(d[key])
                return d
        else:
            return d

    def _desearialize(self, contents):
        contents = json.loads(contents)
        contents = self._desearializeHelper(contents)
        return contents

    def getSharedLock(self):
        return SharedLock(self._fd_readonly)

    def getExclusiveLock(self):
        return ExclusiveLock(self._fd_readonly)

    def save(self):
        attribs = self._serialize()
        with self.getExclusiveLock():
            with open(self._filename, 'w') as fd:
                fd.write(attribs)
 
