import marshal
import os
import secrets
import glob

from . import exceptions as _except
from . import polyfill

from .table import Table
from .document import Document
from .chunk import Chunk
from .autogenerateid import AutoGenerateId
from .console import Console

import atexit

name = "tasho"


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
        open_instead = options.get('open_instead', False)
        if os.path.exists(directory):
            if open_instead:
                return Database.open(directory, **options)
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
        return "<tasho.database: {}>".format(self._directory)

    def _atexit_cleanup(self):
        if self.commit_on_exit:
            dirties = []
            for table in self._tables.values():
                dirties.extend(table.dirty)

            for chunk in dirties:
                print(f"Commiting {chunk}")
                chunk.commit()
            
            Console.log('Waiting for commits to finish.')
            for chunk in dirties:
                chunk.commitQueue.join()


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
                raise _except.DatabaseOperationException("Wrong drop key.")


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
