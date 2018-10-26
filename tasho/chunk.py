import os
import marshal

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
