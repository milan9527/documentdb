from pymongo import MongoClient, ASCENDING, DESCENDING
import concurrent.futures
import threading
import random
import time
import logging
import statistics
from datetime import datetime
from typing import List, Dict
import queue
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.batch = []
        self.batch_lock = threading.Lock()

    def add_and_get_batch(self, item):
        with self.batch_lock:
            self.batch.append(item)
            if len(self.batch) >= self.batch_size:
                result = self.batch
                self.batch = []
                return result
        return None

class ConnectionPool:
    def __init__(self, pool_size=500):
        self.pool_size = pool_size
        self.connections = queue.Queue(maxsize=pool_size)
        self.connection_count = 0
        self.lock = threading.Lock()
        
    def get_connection(self):
        try:
            return self.connections.get_nowait()
        except queue.Empty:
            with self.lock:
                if self.connection_count < self.pool_size:
                    conn = MapElementDB()
                    self.connection_count += 1
                    return conn
            return self.connections.get()

    def return_connection(self, conn):
        self.connections.put(conn)

class MapElementDB:
    def __init__(self):
        username = "milan"
        password = "wAr16dk7"
        host = "ping5.cluster-c7b8fns5un9o.us-east-1.docdb.amazonaws.com"
        port = 27017

        self.timestamps = [
            1711105300562,
            1711005300562,
            1710905300562
        ]

        connection_string = f"mongodb://{username}:{password}@{host}:{port}/?tls=false&retryWrites=false"
        self.client = MongoClient(
            connection_string,
            maxPoolSize=500,
            minPoolSize=100,
            maxIdleTimeMS=120000,
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=3000,
            waitQueueMultiple=100
        )
        self.db = self.client.mapdb
        self.collection = self.db.map_elements
        self._ensure_indexes()

    def _ensure_indexes(self):
        try:
            self.collection.create_index([("tile", 1), ("tsver", -1)])
            self.collection.create_index([("tile", 1), ("element", 1), ("tsver", -1)])
        except Exception as e:
            logger.warning(f"Index creation failed: {str(e)}")

    def get_latest_elements_batch(self, tiles):
        """Process multiple tiles in a single batch operation"""
        selected_timestamp = random.choice(self.timestamps)
        
        pipeline = [
            {
                "$match": {
                    "tile": {"$in": tiles},
                    "tsver": {"$lte": selected_timestamp}
                }
            },
            {
                "$sort": {"tsver": -1}
            },
            {
                "$group": {
                    "_id": {
                        "tile": "$tile",
                        "element": "$element"
                    },
                    "max_tsver": {"$first": "$tsver"}
                }
            }
        ]

        return list(self.collection.aggregate(
            pipeline,
            allowDiskUse=True,
            hint={"tile": 1, "tsver": -1}
        ))

class QueryWorker(threading.Thread):
    def __init__(self, worker_id: int, connection_pool: ConnectionPool, 
                 query_queue: queue.Queue, batch_processor: BatchProcessor,
                 results: List[Dict], duration: int):
        super().__init__()
        self.worker_id = worker_id
        self.connection_pool = connection_pool
        self.query_queue = query_queue
        self.batch_processor = batch_processor
        self.results = results
        self.duration = duration
        self.stop_flag = False

    def run(self):
        conn = self.connection_pool.get_connection()
        start_time = time.time()
        batch = []

        try:
            while not self.stop_flag and (time.time() - start_time) < self.duration:
                try:
                    # Collect tiles for batch processing
                    while len(batch) < 100:  # Process up to 100 tiles per batch
                        tile = self.query_queue.get_nowait()
                        batch.append(tile)
                except queue.Empty:
                    if not batch:
                        time.sleep(0.001)
                        continue

                if batch:
                    query_start = time.time()
                    try:
                        results = conn.get_latest_elements_batch(batch)
                        query_time = (time.time() - query_start) / len(batch)

                        for tile in batch:
                            self.results.append({
                                'worker_id': self.worker_id,
                                'tile': tile,
                                'query_time': query_time,
                                'elements_count': len(results),
                                'success': True,
                                'timestamp': time.time()
                            })
                    except Exception as e:
                        logger.error(f"Batch query error: {str(e)}")
                        for tile in batch:
                            self.results.append({
                                'worker_id': self.worker_id,
                                'tile': tile,
                                'query_time': time.time() - query_start,
                                'elements_count': 0,
                                'success': False,
                                'error': str(e)
                            })
                    finally:
                        for _ in range(len(batch)):
                            self.query_queue.task_done()
                        batch.clear()

        finally:
            self.connection_pool.return_connection(conn)

    def stop(self):
        self.stop_flag = True

def run_benchmark(num_clients: int, duration: int) -> Dict:
    results = []
    query_queue = queue.Queue()
    connection_pool = ConnectionPool(pool_size=min(num_clients, 500))
    batch_processor = BatchProcessor(batch_size=100)
    
    # Create worker threads
    workers = []
    for i in range(num_clients):
        worker = QueryWorker(i, connection_pool, query_queue, batch_processor, results, duration)
        workers.append(worker)
        worker.start()

    # Generate queries
    start_time = time.time()
    while time.time() - start_time < duration:
        tile_num = str(random.randint(0, 1999)).zfill(4)
        tile = f"tile_{tile_num}"
        query_queue.put(tile)

        if (time.time() - start_time) % 1 < 0.1:
            queries_so_far = len(results)
            elapsed = time.time() - start_time
            qps = queries_so_far / elapsed if elapsed > 0 else 0
            logger.info(f"Progress: {elapsed:.1f}s, Queries: {queries_so_far}, QPS: {qps:.1f}")

    # Stop workers and wait for completion
    for worker in workers:
        worker.stop()
    for worker in workers:
        worker.join()

    # Calculate statistics
    if not results:
        return {
            'num_clients': num_clients,
            'duration': duration,
            'total_queries': 0,
            'successful_queries': 0,
            'errors': 0,
            'qps': 0,
            'avg_time': 0,
            'median_time': 0,
            'min_time': 0,
            'max_time': 0,
            'total_elements': 0
        }

    query_times = [r['query_time'] for r in results if r['success']]
    successful_queries = len([r for r in results if r['success']])
    total_elements = sum(r['elements_count'] for r in results if r['success'])
    total_queries = len(results)
    
    # Calculate total QPS across all threads
    qps = total_queries / duration  # Changed from len(results) / duration

    return {
        'num_clients': num_clients,
        'duration': duration,
        'total_queries': total_queries,
        'successful_queries': successful_queries,
        'errors': len(results) - successful_queries,
        'qps': qps,  # This now represents total QPS across all threads
        'avg_time': statistics.mean(query_times) if query_times else 0,
        'median_time': statistics.median(query_times) if query_times else 0,
        'min_time': min(query_times) if query_times else 0,
        'max_time': max(query_times) if query_times else 0,
        'total_elements': total_elements
    }

def main():
    # Test configuration
    client_counts = [100, 500, 1000, 5000, 10000, 40000]
    duration = 60  # seconds per test
    cooldown = 10  # seconds between tests

    print("\nDocumentDB Query Performance Benchmark")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    print(f"Test duration per concurrency level: {duration} seconds")
    print(f"Testing random tiles from tile_0000 to tile_1999")
    print("=" * 80)

    results = []
    for num_clients in client_counts:
        print(f"\nTesting with {num_clients} concurrent clients...")
        result = run_benchmark(num_clients, duration)
        results.append(result)

        print(f"\nResults for {num_clients} clients:")
        print("-" * 60)
        print(f"Total queries: {result['total_queries']}")
        print(f"Successful queries: {result['successful_queries']}")
        print(f"Errors: {result['errors']}")
        print(f"Queries per second: {result['qps']:.2f}")
        print(f"Average query time: {result['avg_time']*1000:.2f} ms")
        print(f"Median query time: {result['median_time']*1000:.2f} ms")
        print(f"Min query time: {result['min_time']*1000:.2f} ms")
        print(f"Max query time: {result['max_time']*1000:.2f} ms")
        print(f"Total elements retrieved: {result['total_elements']}")

        print(f"\nCooling down for {cooldown} seconds...")
        time.sleep(cooldown)

    # Print summary table
    print("\nBenchmark Summary")
    print("=" * 100)
    print("Clients | QPS     | Avg(ms) | Median(ms) | Min(ms) | Max(ms) | Success % | Total Queries")
    print("-" * 100)
    for r in results:
        success_rate = (r['successful_queries'] / r['total_queries'] * 100) if r['total_queries'] > 0 else 0
        print(f"{r['num_clients']:7d} | {r['qps']:7.1f} | {r['avg_time']*1000:7.1f} | "
              f"{r['median_time']*1000:9.1f} | {r['min_time']*1000:7.1f} | {r['max_time']*1000:7.1f} | "
              f"{success_rate:8.1f}% | {r['total_queries']:13d}")
    print("=" * 100)

if __name__ == "__main__":
    main()
