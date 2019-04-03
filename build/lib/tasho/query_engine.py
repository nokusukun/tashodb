from multiprocessing import Process, Pipe, Queue, Manager
import secrets
import dill 

import time

class QueryEngine(object):
    """docstring for QueryEngine"""
    def __init__(self, worker_count):
        self.worker_count = worker_count
        self.queue = Queue()
        self.manager = Manager()
        self.workers = [] 
        
        for i in range(self.worker_count):
            proc = Process(target=QueryEngine._worker, args=(self.queue, i))
            self.workers.append(proc)
            proc.start()


    def _worker(queue, proc_id):
        print(f"Running Worker {proc_id}")
        while True:
            time.sleep(0.1)
            while not queue.empty():
                print(f"[{proc_id}] Activate")
                id, chunk, query, job, blocks = queue.get()
                #print(f"[{proc_id}-{id}]Recieved Job for ({len(chunk.items)}){chunk}")
                query = dill.loads(query)
                blocks[chunk.name] = [x for x in chunk.items.items() if query(x[0], x[1])]
                #print(f"[{proc_id}-{id}]Result ({len(blocks[chunk.name])}){chunk}")
                job['count'] += 1


    def query(self, query, chunks):
        blocks = self.manager.dict()
        job = self.manager.dict()
        job['count'] = 0
        query = dill.dumps(query)

        for chunk in chunks:
            _id = secrets.token_hex(8)
            print(f"[{_id}]Registering Job for {chunk}")
            self.queue.put([_id, chunk, query, job, blocks])

        print("Waiting...")
        while job['count'] < len(chunks):
            time.sleep(0.1)

        return [j for sub in list(blocks.values()) for j in sub]
