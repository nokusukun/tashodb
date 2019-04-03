import os, multiprocessing, dill
import glob, marshal

from . import polyfill
from .autogenerateid import AutoGenerateId

from .document import Document
from .chunk import Chunk

def find(data):
    items, qcode = data
    query = dill.loads(qcode)
    results = []
    for i in items:
        if query(i[0], i[1]):
            results.append(i)
    return results

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


    def initialize_index(self):
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
                        index[field_data].append((chunk.name, id))
                    else:
                        index[field_data] = [(chunk.name, id)]
        
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
        """
        Table.bulk_insert(Dict{id:data}) returns None

        Insert, but in bulk.
        """ 
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
                self.commit()
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
            nugget = chunk.items.get(key, None)
            if nugget:
                return Document((key, nugget), self)
        return None


    def get_indexed(self, index, query):
        return [[self.get(id) for id in ids[0]] for x, ids in self.indexes[index].items() if query(id, x)]


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


    def _hyper_query(self, query):
        """
        Experimental Function, several magnitudes slower atm.
        """
        qcode = dill.dumps(query)
        result = []
        with multiprocessing.Pool(processes=5) as pool:
            result = pool.map(find, [
                    (list(chunk.items.items()), qcode) for chunk in self.chunks
                ]
            )
        return result


    def commit(self):
        """
        Table.commit()

        Writes all of the unsaved changes to the disk.
        """
        for chunk in self.dirty:
           chunk.commit()
        # for chunk in [chunk for chunk in self.chunks if chunk.dirty]:
        #     chunk.commit()

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

