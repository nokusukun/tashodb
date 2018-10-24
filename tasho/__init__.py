import marshal
#import json as marshal
import os
import secrets
import glob

from . import exceptions as _except
from . import polyfill

import atexit

class Database(): 
    """Database.new(String:database_file, **options) returns tasho.database.Database

            Creates a new database object, a folder will be created
            with the same name as the database name.
            Options:
                chunk_size=Int:8192
                    > Table chunk size.
                auto_commit=Bool:False
                    > Commits upon storing data
                        (useful for large insert ops)
            
        Database.open(String:database_file) returns tasho.database.Database

            Opens an existing Database database.


        Database(directory, **options) returns tasho.database.Database
            
            Internaly used by Database, please use Database.new or Database.open respectively."""

    @classmethod
    def new(Database, directory, **options):
        if os.path.exists(directory):
            err = "Database '{}' already exists. Drop the database first.".format(directory)
            raise _except.DatabaseInitException(err)

        os.mkdir(directory)

        properties = {
            "chunk_size": options.get("chunk_size", 8192),
            "table_index": options.get("table_index", "tables"),
            "auto_commit": options.get("auto_commit", False)
        }

        with open(os.path.join(directory, "properties"), "wb") as f:
            marshal.dump(properties, f)

        with open(os.path.join(directory, properties['table_index']), "wb") as f:
            marshal.dump({}, f)

        return Database(directory, **properties)

    @classmethod
    def open(Database, directory, append=True, **options):
        if not os.path.exists(directory):
            if append:
                return Database.new(directory, **options)
            else:
                err = "Database '{}' does not exist.".format(directory)
                raise _except.DatabaseInitException(err)

        with open(os.path.join(directory, 'properties'), "rb") as f:
            properties = marshal.load(f)

        return Database(directory, **properties)


    def _load_internal(self, filename):
        with open(os.path.join(self._directory, filename), "rb") as f:
            return marshal.load(f)


    def _write_internal(self, filename, data):
        with open(os.path.join(self._directory, filename), "wb") as f:
            marshal.dump(data, f)


    def __init__(self, directory, **options):
        self._options = options
        self._directory = directory
        self._table_index = self._load_internal(options['table_index'])
        self._database = {}
        self._tables = {}
        self.commit_on_exit = True
        for table_i, chunks in self._table_index.items():
            self._tables[table_i] = Table(table_i, 
                                          directory, 
                                          chunks, 
                                          self._options.get('auto_commit'),
                                          self._options.get('chunk_size'), 
                                          self)

        atexit.register(self._atexit_cleanup)

    def __repr__(self):
        return "<tasho.database: {}>".format(self.directory)

    def _atexit_cleanup(self):
        if self.commit_on_exit:
            dirties = []
            for table in self._tables.values():
                dirties.extend(table.dirty)

            for chunk in dirties:
                print(f"Commiting {chunk}")
                chunk.commit()

    @property
    def table(self):
        return TableSelector(self)

    @property
    def tables(self):
        return self._tables

    def get_table(self, table_name):
        """
        Database.get_table(String:table_name) returns tasho.database.Table
            Returns a table object. Creates a new table if it doesn't exist.
            You can also call the table though `Database.table.table_name`
        """
        if table_name in self._tables:
            return self._tables[table_name]
        else:
            return self.new_table(table_name)

    def new_table(self, table_name):
        if table_name in self._table_index:
            raise _except.DatabaseInitException(
                    "Table '{}' already exists. Drop the table first.".format(table_name))

        table = Table(table_name, 
                      self._directory, 
                      [], 
                      self._options.get('auto_commit'), 
                      self._options.get('chunk_size'), 
                      self)

        table._new_chunk()
        self._tables[table.name] = table
        self.commit_table_index()
        return table

    def drop_table(self, table_name, drop_key):
        """
        Database.drop_table(String:table_name, String:drop_key)
            Deletes a table. You must supply the table's drop key
            which can be found through `Table.drop_key`.
        """
        if table_name in self._table_index:
            if self._tables[table_name].drop_key == drop_key:
                chunks = self._table_index.pop(table_name)
                table = self._tables.pop(table_name)
                table.__is_dropped = True
                for chunk in chunks:
                    os.remove(os.path.join(self._directory, chunk))
            else:
                raise DatabaseOperationException("Wrong drop key.")


    def commit_table_index(self):
        self._table_index = {table.name: table.chunk_ids for table in self._tables.values()}
        self._write_internal(self._options['table_index'], self._table_index)


class TableSelector():
    def __init__(self, database):
        self.db = database

    def __getattr__(self, table_name):
        if table_name in self.db._tables:
            return self.db._tables[table_name]
        else:
            return self.db.new_table(table_name)

    def __getitem__(self, table_name):
        if table_name in self.db._tables:
            return self.db._tables[table_name]
        else:
            return self.db.new_table(table_name)


class Table():

    def __init__(self, table_name, path, chunk_ids = [], auto_commit=True, chunk_size=8192, db=None):
        self.name = table_name
        self.path = path
        self.chunks = []
        self.chunk_size = chunk_size
        self.auto_commit = True
        self.db = db
        self.__is_dropped = False
        self.indexes = {}

        for c_id in chunk_ids:
            chunk_path = os.path.join(self.path, c_id)
            self.chunks.append(
                    Chunk(c_id, chunk_path, self.chunk_size))



    def __repr__(self):
        return "{is_dropped}<TashoDBTable:{name} Chunks: {chunkcount}>".format(
                    name=self.name,
                    chunkcount=len(self.chunks),
                    is_dropped='DROPPED' if self.__is_dropped else '')


    def initalize_index(self):
        for index in glob.glob(os.path.join(self.path, "{}-*.index".format(self.name))):
            with open(index, "rb") as f:
                self.indexes.update(marshal.loads(f.read()))

    def create_index(self, field):
        index = {}
        for chunk in self.chunks:
            for id, document in chunk.items.items():
                field_data = document.get(field, None)
                if field_data:
                    if index.get(field_data):
                        index[field_data].append([chunk.name, id])
                    else:
                        index[field_data] = [[chunk.name, id]]
        
        with open(os.path.join(self.path, "{}-{}.index".format(self.name, field)), "wb") as f:
            f.write(marshal.dumps({field: index}))

        self.indexes.update({field: index})

    @property
    def active_chunk(self):
        """
        Table.active_chunk returns tasho.database.Chunk
        Returns the last chunk that can still be writable.
        """
        return self.chunks[-1]

    @property
    def drop_key(self):
        return "DROP{}{}{}".format(self.name, self.chunk_size, self.path)

    @property
    def chunk_ids(self):
        """
        Table.chunk_ids returns list
        Returns the chunk IDs used by the database object.
        """
        return [x.name for x in self.chunks]


    @property
    def dirty(self):
        return [x for x in self.chunks if x.dirty]


    def items(self):
        """
        Table.items() returns (String/int:id, Dict:document)
        Returns a generator going through all of the items in the table.
        """
        for i in range(len(self.chunks) -1, -1, -1):
            for item in self.chunks[i].items.items():
                yield item


    def bulk_insert(self, data):
        c_auto_commit = self.auto_commit
        self.auto_commit = False 
        for _id, value in data.items():
            self.insert(_id, value)

        self.commit()
        self.auto_commit = c_auto_commit


    def insert(self, key, value):
        """
        Table.insert(String/Int:key, Dict:value) returns String

        Adds a document to the table. If Table.auto_commit is
        set to true, then the whole table gets commited to disk.
        Returns the chunk name.
        """
        if key == AutoGenerateId:
            key = polyfill.hex_token(8)

        chunk = self.get_chunk(key)
        if chunk:
            chunk.write(key, value, self.auto_commit)
        else:
            if self.active_chunk.is_full:
                self._new_chunk()
            self.active_chunk.write(key, value, self.auto_commit)
        
        return self.get(key)


    def new_document(self, key, value):
        """
        Table.new_document(String/Int:key, Dict:value) returns Document

        Works the same way as Table.insert, but returns 
        a Document object instead.
        """
        self.insert(key, value)
        return self.get(key)


    def delete(self, key):
        """
        Table.delete(String/Int:key) returns Bool

        Deletes a document using a specified ID.
        Documents are usually deleted through Document.delete().
        Returns True if the tablew as sucessfully deleted.
        """
        chunk = self.get_chunk(key)
        if chunk:
            return chunk.delete(key)
        return False


    def raw_get(self, key):
        """
        Table.raw_get(String/Int:key) returns Dict

        Retrieves a document in it's dictonary form] as the document.
        """
        for chunk in self.chunks:
            if key in chunk.items:
                return chunk.items[key]
        return None


    def get(self, key):
        """
        Table.get(String/Int:key) returns tasho.database.Document

        Retrieves and returns a Document object.
        """
        for chunk in self.chunks:
            nugger = chunk.items.get(key, None)
            if nugger:
                return Document((key, nugger), self)
        return None


    def get_indexed(self, index, query):
        return [[self.get(id) for id in ids[0]] for x, ids in self.indexes[index].items() if x == query]


    def query(self, query):
        """
        Table.query(function(id, document)) returns List[tasho.database.Document]

        Queries the table using the callable as the filter.
        Ex. Table.query(lambda id, document: document['age'] > 50)
            - Returns all documents with the 'age' property greater than 50.
        """
        return [Document(x, self) for x in self.items() if query(x[0], x[1])]


    def query_one(self, query):
        """
        Table.query_one(function(id, document)) returns tasho.database.Document

        Same as Table.query but stops at the first match.
        """
        for data in self.items():
            if query(data[0], data[1]):
                return Document(data, self)

    def commit(self):
        """
        Table.commit()

        Writes all of the unsaved changes to the disk.
        """
        for chunk in self.dirty:
            chunk.commit()

        if self.db:
            self.db.commit_table_index()


    # ========== INTERNAL FUNCTIONS =============
    def get_chunk(self, key):
        for chunk in self.chunks:
            if key in chunk.items:
                return chunk
        return None

    def get_chunk_from_name(self, name):
        for chunk in self.chunks:
            if name == chunk.name:
                return chunk
        return None

    def _new_chunk(self):
        chunk_name = self.name + "-" +  polyfill.hex_token(8)
        chunk_path = os.path.join(self.path, chunk_name)
        chunk = Chunk(chunk_name, chunk_path, self.chunk_size)
        chunk.initalize()
        self.chunks.append(chunk)
        return chunk_name



class Document():

    def __init__(self, data, table):
        super(Document, self).__setattr__('_id', data[0])
        super(Document, self).__setattr__('_data', data[1])
        super(Document, self).__setattr__('_table', table)

    def __repr__(self):
        return "<TashoDBDocument:{} Origin: {}>".format(self._id, self._table.name)

    @property
    def dict(self):
        data = {x: y for x, y in self._data.items()}
        data['_id'] = self._id
        return data

    def __getattr__(self, attribute):
        if attribute in self._data:
            return self._data[attribute]

    def __setattr__(self, attribute, data):
        if attribute in self._data:
            self._data[attribute] = data

    def __getitem__(self, attribute):
        return self._data[attribute]
    
    def __setitem__(self, attribute, data):
        self._data[attribute] = data   


    def save(self):
        """
        Document.save()

        Saves the document to the table. Might have to call Table.commit()
        """
        return self._table.insert(self._id, self._data)

    def update(self, data):
        """
        Document.update(Dict:data)

        Updates the document. Works the same as Dict.update()
        """
        self._data.update(data)

    def pop(self, data):
        """
        Document.pop() returns Something

        Works the same way as Dict.pop()
        """
        return self._data.pop()

    def get(self, data, default=None):
        """
        Document.get(Object:data, Object:default)

        Works the same way as Dict.get()
        """
        return self._data.get(data, default)

    def delete(self):
        """
        Document.delete()

        Deletes the document.
        """
        return self._table.delete(self._id)


class Chunk():
    def __init__(self, chunk_id, chunk_path, max_size=8192):
        self.name = chunk_id
        self.chunk_path = chunk_path
        self.max_size = max_size
        self.is_loaded = False
        self._data = {}
        self.idhash = None
        self.dirty = False

    def __repr__(self):
        return "<TashoDBTableChunk:" + self.name + ">"

    def initalize(self):
        if os.path.exists(self.chunk_path):
            with open(self.chunk_path, "rb") as f:
                self._data = marshal.load(f)
                self.idhash = set(self._data.keys())
                self.is_loaded = True

    @property
    def is_full(self):
        if len(self.items) >= self.max_size:
            return True
        else:
            return False

    @property
    def items(self):
        if not self._data:
            self.initalize()
        return self._data

    def index_in_chunk(self, index, data):
        if not self.is_loaded:
            self.initalize()

        if _id in self.idhash:
            return self._data[_id]
        else:
            return None

    def write(self, key, value, commit=False):
        self._data[key] = value
        self.dirty = True
        if commit:
            self.commit()
            self.dirty = False

    def delete(self, key):
        if key in self._data:
            self._data.pop(key)
            self.dirty = True
            return True
        return False

    def commit(self):
        with open(self.chunk_path, "wb") as f:
            marshal.dump(self._data, f)
        self.dirty = False


class AutoGenerateId():
    pass