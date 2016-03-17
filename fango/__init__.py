from pymongo import MongoClient
from functools import partial
import inspect
import datetime
import base64
import getpass
import re


DEFAULT_DB = 'test'
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 27017
DEFAULT_HOSTURL = 'mongodb://localhost:27017/'


def clean_path(path):
    return re.sub(r'[/]+', '/', re.sub(r'^[/]*|[/]*$', '', path))


def methods_with_decorator(cla, decorator_name):
    classes = [cla] + list(cla.__bases__)
    for cl in classes:
        source_lines = inspect.getsourcelines(cl)[0]
        for i, line in enumerate(source_lines):
            line = line.strip()
            if line.split('(')[0].strip() == '@'+decorator_name:  # leaving a bit out
                next_line = source_lines[i+1]
                name = next_line.split('def')[1].split('(')[0].strip()
                yield(name)


class FangoWrapper(object):
    def __init__(self, client, dbname):
        self.client = client
        self.dbname = dbname
        self.db = client[self.dbname]

    def new(self, obj_class, *args, **kwargs):
        """ Method to create FangoObjects, always use this
        method to create such objects eg. self.db.new() or fango.connect().new()
        new( ClassName, path, *args, **kwargs)
        """
        args = tuple([self]+list(args))
        if len(args) > 1:
            path = clean_path(args[1])
            if path:
                collection = '/'.join(path.split('/')[:-1])
                if not collection:
                    collection = '/'
                name = path.split('/')[-1]
                res = self.find_one(collection, {'Name': name})
                if res:
                    kwargs.update(res)
        return obj_class(self, *args, **kwargs)

    def insert(self, documents):
        try:
            iter(documents)
        except TypeError:
            collection = self.db[documents.path]
            if '_id' in documents._metaData:
                collection.update({'_id': documents._metaData['_id']}, documents.serialize(write=True))
            else:
                documents._metaData['_id'] = collection.insert(documents.serialize(write=True))
            return True
        new = {}
        collections = {}
        for d in documents:
            if '_id' in d._metaData:
                try:
                    collection = collections[documents.path]
                except KeyError:
                    collection = collections[documents.path] = self.db[documents.path]
                collection.update({'_id': documents.metadata['_id']}, documents.serialize(write=True))
            else:
                try:
                    doclist = new[documents.path]
                except KeyError:
                    doclist = new[documents.path] = []
                doclist.append(d)
        for k, v in new:
            res = self.db[k].insert([d.serialize(write=True) for d in v])
            for i in xrange(len(v)):
                v[i].metadata['_id'] = res[i]
        return True

    def find_one(self, collection, *args, **kwargs):
        return self.db[collection].find_one(*args, **kwargs)


class FangoConnections(object):
    """ A singleton database connections cache """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(FangoConnections, cls).__new__(cls)
        try:
            cache = cls._instance.__clientcache
        except AttributeError:
            cache = cls._instance.__clientcache = {}

        host = args[0] if len(args) > 0 else kwargs['host'] if 'host' in kwargs else DEFAULT_HOST if len(args) > 2 or 'port' in kwargs else DEFAULT_HOSTURL
        port = args[1] if len(args) > 2 or (len(args) > 1 and 'dbname' in kwargs) else kwargs['port'] if 'port' in kwargs else DEFAULT_PORT
        dbname = args[2] if len(args) > 2 else kwargs['dbname'] if 'dbname' in kwargs else DEFAULT_DB

        if len(args) > 2 or (len(args) > 1 and 'dbname' in kwargs) or 'port' in kwargs:
            key = (host, port)
        else:
            key = (host,)
        try:
            client = cache[key]
        except KeyError:
            if len(key) == 2:
                client = cache[key] = MongoClient(key[0], key[1])
            else:
                ### TODO: implement host aliases with authentication
                client = cache[key] = MongoClient(key[0])

        try:
            cache = cls._instance.__wrappercache
        except AttributeError:
            cache = cls._instance.__wrappercache = {}
        key = tuple(list(key) + [dbname])
        try:
            res = cache[key]
        except KeyError:
            res = cache[key] = FangoWrapper(client, dbname)
        return res


class connect():
    def __init__(self, *args, **kw):
        self.db = FangoConnections(*args, **kw)

    def __enter__(self):
        return self.db

    def __exit__(self, t, value, traceback):
        pass


class FangoField(object):
    def __init__(self, func, doc=None):
        self.func = func
        self.__doc__ = doc if doc is not None else func.__doc__

    def __get__(self, obj=None, objtype=None):
        if obj is None:
            return self.func
        return partial(self, obj)

    def __set__(self, obj, value):
        obj._STOREDFIELDS[self.func.__name__] = value

    def __call__(self, *args, **kw):
        obj = args[0]
        try:
            stored_cache = obj._STOREDFIELDS
        except AttributeError:
            stored_cache = obj._STOREDFIELDS = {}
        if args[1:] or kw:
            try:
                cache = obj.__cache
            except AttributeError:
                cache = obj.__cache = {}
            key = (self.func, args[1:], frozenset(kw.items()))
            try:
                res = cache[key]
            except KeyError:
                res = cache[key] = self.func(*args, **kw)
                stored_cache[self.func.__name__] = res
        else:  # last computed value
            try:
                res = stored_cache[self.func.__name__]
            except KeyError:  # no argument fields doesn't have __cache and use stored_cache as memoized cache instead
                res = stored_cache[self.func.__name__] = self.func(*args, **kw)
        return res


class FangoStoredField(FangoField):
    """Name holder class used to save objects to MongoDB Documents"""
    pass


class FangoObject(object):
    """Main Fango interface class"""
    @FangoStoredField
    def Name(self):
        return base64.b64encode('%s%s' % (self.path, self.__hash__()))

    @FangoStoredField
    def Id(self):
        return self._metaData['_id'] if '_id' in self._metaData else None

    def __init__(self, *args, **kw):
        #super(FangoObject, self).__init__()
        self._STOREDFIELDS = {}
        self.db = args[0] if args else FangoConnections()
        path = False
        if len(args) > 2:
            path = clean_path(args[2])
        elif kw.get('_safePath', False):
            path = kw.get('_safePath', False)
        if path:
            self.path = '/'.join(path.split('/')[:-1])
            if not self.path:
                self.path = '/'
            kw.update({'Name': path.split('/')[-1]})
        else:
            self.path = clean_path('tmp/%s' % (self.__class__.__name__,))
            self._STOREDFIELDS.update({'Name': self.Name()})
        for f in methods_with_decorator(self.__class__, 'FangoStoredField'):
            if f in kw:    
                self._STOREDFIELDS[f] = kw[f]
        self._metaData = {
            '_created_by': kw.get('_created_by', getpass.getuser()),
            '_created_at': kw.get('_created_at', datetime.datetime.utcnow()),
            '_updated_by': kw.get('_updated_by', getpass.getuser()),
            '_updated_at': kw.get('_updated_at', datetime.datetime.utcnow()),
        }
        if '_id' in kw:
            self._metaData['_id'] = kw['_id']

    def serialize(self, write=False):
        out = {}
        for f in methods_with_decorator(self.__class__, 'FangoStoredField'):
            out[f] = getattr(self, f)()
        if write:
            self._metaData['_updated_by'] = getpass.getuser()
            self._metaData['_updated_at'] = datetime.datetime.utcnow()
            out.update(self._metaData)
            out.pop('_id', None)
        else:
            out.update(self._metaData)
        return out

    def insert(self, *args, **kwargs):
        path = kwargs.get('path', False)
        if path:
            path = clean_path(path)
            self.path = '/'.join(path.split('/')[:-1])
            self.Name = path.split('/')[-1]
        self.db.insert(self, *args, **kwargs)

    def copy(self, path=None):
        return self.db.new(self.__class__, path, **self._STOREDFIELDS)

if __name__ == "__main__":
    class Test(FangoObject):
        @FangoStoredField
        def CreationTime(self):
            return datetime.datetime.now()

        @FangoStoredField
        def Data(self):
            return {}

    with connect(dbname='test') as db:
        print "Creating Test Object"
        obj = db.new(Test, 'a', Data={'1': 0})
        obj.insert()
        obj = db.new(Test, Data={'2': 0})
        obj.insert()
        obj2 = obj.copy('c')
        obj2.insert()
        print "Success"
