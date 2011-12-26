import datetime
import pickle
import fcntl
import os
import errno
import shutil
import types
import pprint
import errors
import random
import string

def getFastestJSONModule():
    try:
        module = __import__('ujson')
        return module
    except ImportError:
        pass

    try:
        module = __import__('cjson')
        class json(object):
            loads = module.decode
            dumps = module.encode
        return json()
    except ImportError:
        pass

    try:
        module = __import__('json')
        return module
    except ImportError:
        raise ImportError('No acceptable json module found.')

json = getFastestJSONModule()


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

    def __del__(self):
        self.releaseLock()


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
                raise errors.TableAlreadyExists(table)
            else:
                raise

    def removeTable(self, table):
        try:
            shutil.rmtree(os.path.join(self._data_dir, self._db, table))
        except OSError:
            if e.errno == errno.ENOENT: # if the table doesn't exist
                raise errors.TableDoesNotExist(table)
            else:
                raise


class Table(NoDBBase):
    def __init__(self, data_dir, db, table):
        self._data_dir = data_dir
        self._db = db
        self._table = table
        self._fd_lock = open(os.path.join(self._data_dir, self._db, self._table, '.lock'), 'w')

    def get(self, key):
        """Returns a Row. In a multi-process / multi-threaded application, getReadOnly(key) and
        getLocked(key) should be used to prevent race conditions."""
        return Row(self._data_dir, self._db, self._table, key)

    def getReadOnly(self, key):
        """Returns a ReadOnlyRow (with no locks). A ReadOnlyRow behaves like a Row except
        that it is not writable."""
        return ReadOnlyRow(self._data_dir, self._db, self._table, key)

    def getLocked(self, key):
        """Returns a LockedRow. The lock is released when the object is deleted, or all references
        to it are released."""
        return LockedRow(self._data_dir, self._db, self._table, key)

    def create(self, key):
        with self.getExclusiveLock():
            row = Row(self._data_dir, self._db, self._table, key, is_new=True)
        return row

    def createWithUniqueKey(self, key_len=5):
        while True:
            try:
                key = self._generateRandomString(key_len)
                row = self.create(key)
                break
            except RowAlreadyExists:
                continue

        return row

    def createLocked(self, key):
        with self.getExclusiveLock():
            row = LockedRow(self._data_dir, self._db, self._table, key, is_new=True)
        return row

    def createLockedWithUniqueKey(self, key_len=5):
        while True:
            try:
                key = self._generateRandomString(key_len)
                row = self.createLocked(key)
                break
            except errors.RowAlreadyExists:
                continue

        return row

    def remove(self, key):
        try:
            os.remove(os.path.join(self._data_dir, self._db, self._table, key))
        except IOError as e:
            if e.errno == errno.ENOENT: # if the file doesn't exist
                raise errors.RowDoesNotExist(key)
            else:
                raise

    def _generateRandomString(self, length=5):
        return ''.join([random.choice(string.ascii_letters + string.digits) for i in range(length)])

class RowBase(NoDBBase):
    def __init__(self, data_dir, db, table, key, is_new=False):
        self._data_dir = data_dir
        self._db = db
        self._table = table
        self._key = key
        self._filename = os.path.join(self._data_dir, self._db, self._table, key)

        if is_new: # note: must be run inside a table lock
            if os.path.exists(self._filename):
                raise errors.RowAlreadyExists(key)
            with open(self._filename, 'w') as fd:
                fd.write('{}') # touch the file so we can lock it later, and fill it with an empty JSON dict

        try:
            self._fd_readonly = self._fd_lock = open(self._filename, 'r')
        except IOError as e:
            if e.errno == errno.ENOENT: # if the file doesn't exist
                raise errors.RowDoesNotExist(key)
            else:
                raise

        if self._is_locked:
            self.getExclusiveLock().acquireLock()

        if not is_new:
            self._loadContents()

    def _loadContents(self):
        self._fd_readonly.seek(0)
        if not self._is_locked:
            with self.getSharedLock():
                contents = self._fd_readonly.read()
        else:
            contents = self._fd_readonly.read()
        contents = self._desearialize(contents)
        self.__dict__.update(contents)

    def __repr__(self):
        attribs = self._getPublicAttribs()
        return '<NoDB.Row object - key: %s>\n\n%s' % (self._key, pprint.pformat(attribs))

    def _getPublicAttribs(self):
        return dict([(key, value) for key, value in self.__dict__.items() if key[0] != '_'])

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

    def getCreated(self):
        return datetime.datetime.fromtimestamp(os.path.getctime(self._filename))

    def getModified(self):
        return datetime.datetime.fromtimestamp(os.path.getmtime(self._filename))

    def getKey(self):
        return self._key


class WritableRowBase(RowBase):
    def save(self):
        attribs = self._serialize()
        with self.getExclusiveLock():
            with open(self._filename, 'w') as fd:
                fd.write(attribs)

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

    def _serialize(self):
        attribs = self._getPublicAttribs()
        attribs = self._serializeHelper(attribs)
        attribs = json.dumps(attribs)
        return attribs


class Row(WritableRowBase):
    def __init__(self, *args, **kwargs): 
        self._is_locked = False
        super(Row, self).__init__(*args, **kwargs)
 

class ReadOnlyRow(RowBase):
    def __init__(self, *args, **kwargs): 
        self._is_locked = False
        super(ReadOnlyRow, self).__init__(*args, **kwargs)


class LockedRow(WritableRowBase):
    def __init__(self, *args, **kwargs): 
        self._is_locked = True
        super(LockedRow, self).__init__(*args, **kwargs)
