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

class Lock(object):
    def __init__(self, fd):
        self.fd = fd
        self.state = '' # can be 'shared', 'exclusive', 'saving', or '' (no lock)

    def acquireSharedLock(self):
        fcntl.flock(self.fd, fcntl.LOCK_SH)
        self.state = 'shared'

    def acquireExclusiveLock(self):
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        self.state = 'exclusive'

    def releaseLock(self):
        self.state = ''
        fcntl.flock(self.fd, fcntl.LOCK_UN)

    def getExclusiveLockWrapper(self):
        return ExclusiveLockWrapper(self)

    def getSharedLockWrapper(self):
        return SharedLockWrapper(self)


class ExclusiveLockWrapper(object):
    def __init__(self, lock):
        self.lock = lock

    def __enter__(self):
        self.lock.acquireExclusiveLock()

    def __exit__(self, type, value, traceback):
        self.lock.releaseLock()


class SharedLockWrapper(object):
    def __init__(self, lock):
        self.lock = lock

    def __enter__(self):
        self.lock.acquireSharedLock()

    def __exit__(self, type, value, traceback):
        self.lock.releaseLock()


class NoDBBase(object):
    def __del__(self):
        self.lock.releaseLock()
        self._fd_lock.close()


class Database(NoDBBase):
    def __init__(self, data_dir, db):
        self._data_dir = data_dir
        self._db = db
        self._fd_lock = open(os.path.join(self._data_dir, self._db, '.lock'), 'w')
        self.lock = Lock(self._fd_lock)

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
        self.lock = Lock(self._fd_lock)

    def getRow(self, key, lock_type=None):
        row = Row(self._data_dir, self._db, self._table, key, lock_type)

    def createRow(self, key, lock=None): # lock can be 'shared' or 'exclusive'
        with self.lock.ExclusiveLockWrapper():
            filename = os.path.join(self._data_dir, self._db, self._table, key)
            if os.path.exists(filename):
                raise errors.RowAlreadyExists(key)
            with open(filename, 'w') as fd:
                fd.write('{}') # touch the file so we can lock it later, and fill it with an empty JSON dict
            row = Row(self._data_dir, self._db, self._table, key, lock_type)

        return row

    def createRowWithUniqueKey(self, key_len=5, lock_type=None):
        while True:
            try:
                key = self._generateRandomString(key_len)
                row = self.createRow(key, lock_type)
                break
            except RowAlreadyExists:
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


class Row(NoDBBase):
    def __init__(self, data_dir, db, table, key, lock_type=None):
        self._data_dir = data_dir
        self._db = db
        self._table = table
        self._key = key
        self._filename = os.path.join(self._data_dir, self._db, self._table, key)
        self._fd_readonly = self._fd_lock = open(self._filename, 'r')
        self.lock = Lock(self._fd_readonly)

        if lock_type == 'shared':
            self.lock.acquireSharedLock()
        elif lock_type == 'exclusive':
            self.lock.acquireExclusiveLock()

        self._loadContents()

    def _loadContents(self):
        self._fd_readonly.seek(0)
        if self.lock.state = '':
            self.lock.acquireSharedLock()
            contents = self._fd_readonly.read()
            self.lock.releaseLock()
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

    def save(self):
        if self.lock.state == '':
            self.lock.acquireExclusiveLock()
            self._writeContents()
            self.lock.releaseLock()
        elif self.lock.state == 'shared':
            self.lock.acquireExclusiveLock()
            self._writeContents()
            self.acquireSharedLock()
        elif self.lock.state == 'exclusive':
            self._writeContents()
        else:
            raise RuntimeError('Invalid lock type.')

    def _writeContents(self)
        attribs = self._serialize()
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
