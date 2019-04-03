import os
import marshal
import threading
import time
import queue

from .console import Console

def commitManager(commitQueue, chunk_path):
    try:
        while True:
            data = commitQueue.get(True, 15)
            flattenCount = 0
            while not commitQueue.empty():
                flattenCount += 1
                data.update(commitQueue.get())
            # Console.log(f'[{chunk_path}]Flattened Commit Count:', flattenCount)
            with open(chunk_path, "wb") as f:
                marshal.dump(data, f)
    except queue.Empty:
        chunkName = chunk_path.split("/")[-1].split("\\")[-1]
        Console.log(f'[{chunkName}]Retiring Manager')

class Chunk():
    def __init__(self, chunk_id, chunk_path, max_size=8192):
        self.name = chunk_id
        self.chunk_path = chunk_path
        self.max_size = max_size
        self.is_loaded = False
        self._data = {}
        self.idhash = None
        self.dirty = False
        self.commitQueue = queue.Queue()
        self.commitThread = None
        Console.log(f'[{self.name}] Lazy loaded')


    def __repr__(self):
        return "<TashoDBTableChunk:" + self.name + ">"

    def initalize(self):
        if os.path.exists(self.chunk_path):
            with open(self.chunk_path, "rb") as f:
                self._data = marshal.load(f)
                self.idhash = set(self._data.keys())
                self.is_loaded = True    
            Console.log(f'[{self.name}] Fully loaded')

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

        if index in self.idhash:
            return self._data[index]
        else:
            return None

    def find(self, query):
        results = []
        for i in self.items:
            if query(i[0], i[1]):
                results.append(i)
        return results

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
        if not self.commitThread or not self.commitThread.isAlive():
            Console.log(f'[{self.name}]Spawning Thread')
            self.commitThread = threading.Thread(
                None, 
                target=commitManager, 
                args=(self.commitQueue, self.chunk_path), 
                daemon=True
            )
            self.commitThread.start()

        self.commitQueue.put(self._data)
        self.dirty = False
