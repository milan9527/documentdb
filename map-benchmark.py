from pymongo import MongoClient, ASCENDING, DESCENDING
import concurrent.futures
import random
import time
import logging
import statistics
from datetime import datetime
from typing import List, Dict
from queue import Queue, Empty
from threading import Event

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MapElementDB:
    _instance = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MapElementDB, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not MapElementDB._initialized:
            username = "milan"
            password = "xxxx"
            host = "ping5.cluster-c7b8fns5un9o.us-east-1.docdb.amazonaws.com"
            port = 27017

            connection_string = f"mongodb://{username}:{password}@{host}:{port}/?tls=false&retryWrites=false"
            self.client = MongoClient(
                connection_string,
                maxPoolSize=100,  # Adjust based on your needs
                minPoolSize=10,
                maxIdleTimeMS=45000,
                waitQueueTimeoutMS=1000
            )
            self.db = self.client["mapdb"]
            self.collection = self.db["map_elements"]
            self._ensure_indexes()
            MapElementDB._initialized = True

    def _ensure_indexes(self):
        try:
            self.collection.create_index([("tile", 1), ("tsver", -1)])
            self.collection.create_index([("tile", 1), ("element", 1), ("tsver", -1)])
        except Exception as e:
            logger.warning(f"Index creation failed: {str(e)}")

    def get_latest_elements(self, tile, max_timestamp):
        pipeline = [
            {
                "$match": {
                    "tile": tile,
                    "tsver": {"$lte": 1711105300562}
                }
            },
            {
                "$sort": {"tsver": -1}
            },
            {
                "$group": {
                    "_id": "$element",
                    "max_tsver": {"$first": "$tsver"}
                }
            },
            {
                "$project": {
                    "element": "$_id",
                    "max_tsver": 1,
                    "_id": 0
                }
            }
        ]
        
        return list(self.collection.aggregate(
            pipeline,
            allowDiskUse=True,
            hint={"tile": 1, "tsver": -1}
        ))

def perform_query(db, tile):
    """Execute a single query and return its execution time"""
    start_time = time.time()
    try:
        results = db.get_latest_elements(tile, None)
        query_time = time.time() - start_time
        return {
            'tile': tile,
            'query_time': query_time,
            'elements_count': len(results),
            'success': True
        }
    except Exception as e:
        return {
            'tile': tile,
            'query_time': time.time() - start_time,
            'elements_count': 0,
            'success': False,
            'error': str(e)
        }

def worker(result_queue: Queue, stop_event: Event):
    """Worker function that performs queries"""
    db = MapElementDB()
    while not stop_event.is_set():
        tile_num = str(random.randint(0, 1999)).zfill(4)
        tile = f"tile_{tile_num}"
        result = perform_query(db, tile)
        result_queue.put(result)

def run_benchmark(num_workers: int, duration: int) -> Dict:
    """Run benchmark with specified number of workers"""
    results = []
    result_queue = Queue()
    stop_event = Event()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(worker, result_queue, stop_event)
            for _ in range(num_workers)
        ]

        start_time = time.time()
        last_progress = start_time

        try:
            while time.time() - start_time < duration:
                try:
                    result = result_queue.get(timeout=0.1)
                    results.append(result)

                    current_time = time.time()
                    if current_time - last_progress >= 1.0:
                        elapsed = current_time - start_time
                        qps = len(results) / elapsed
                        logger.info(f"Progress: {elapsed:.1f}s, Queries: {len(results)}, QPS: {qps:.1f}")
                        last_progress = current_time
                except Empty:
                    continue
        finally:
            stop_event.set()
            for future in futures:
                future.cancel()

    # Calculate statistics
    if not results:
        return {
            'num_workers': num_workers,
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
    actual_duration = time.time() - start_time

    return {
        'num_workers': num_workers,
        'duration': actual_duration,
        'total_queries': len(results),
        'successful_queries': successful_queries,
        'errors': len(results) - successful_queries,
        'qps': len(results) / actual_duration,
        'avg_time': statistics.mean(query_times) if query_times else 0,
        'median_time': statistics.median(query_times) if query_times else 0,
        'min_time': min(query_times) if query_times else 0,
        'max_time': max(query_times) if query_times else 0,
        'total_elements': total_elements
    }

def main():
    # Test configuration
    worker_counts = [100, 500, 1000, 2000]  # Reduced max concurrency
    duration = 30  # Reduced duration
    cooldown = 5   # Reduced cooldown

    print("\nDocumentDB Query Performance Benchmark")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    print(f"Test duration per concurrency level: {duration} seconds")
    print("=" * 80)

    results = []
    for num_workers in worker_counts:
        print(f"\nTesting with {num_workers} concurrent workers...")
        result = run_benchmark(num_workers, duration)
        results.append(result)

        print(f"\nResults for {num_workers} workers:")
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

        if num_workers < worker_counts[-1]:
            print(f"\nCooling down for {cooldown} seconds...")
            time.sleep(cooldown)

    # Print summary table
    print("\nBenchmark Summary")
    print("=" * 100)
    print("Workers | QPS     | Avg(ms) | Median(ms) | Min(ms) | Max(ms) | Success % | Total Queries")
    print("-" * 100)
    for r in results:
        success_rate = (r['successful_queries'] / r['total_queries'] * 100) if r['total_queries'] > 0 else 0
        print(f"{r['num_workers']:7d} | {r['qps']:7.1f} | {r['avg_time']*1000:7.1f} | "
              f"{r['median_time']*1000:9.1f} | {r['min_time']*1000:7.1f} | {r['max_time']*1000:7.1f} | "
              f"{success_rate:8.1f}% | {r['total_queries']:13d}")

if __name__ == "__main__":
    main()
