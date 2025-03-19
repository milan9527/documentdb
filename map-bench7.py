from pymongo import MongoClient
import threading
import random
import time
import statistics
from datetime import datetime
from typing import List, Dict
import os
import logging

# Suppress MongoDB logs
logging.getLogger("pymongo").setLevel(logging.ERROR)

class MapElementDB:
    def __init__(self):
        username = "milan"
        password = "wAr16dk7"
        host = "ping5.cluster-c7b8fns5un9o.us-east-1.docdb.amazonaws.com"
        port = 27017

        self.timestamps = [
            1711105300562,
            1711005300562,
            1710905300562,
            1710904300562,
            1710903300562,
            1710902300562,
            1710901300562,
            1710900300562
        ]

        # Modified connection settings
        connection_string = f"mongodb://{username}:{password}@{host}:{port}/?tls=false&retryWrites=false"
        self.client = MongoClient(
            connection_string,
            maxPoolSize=100,
            minPoolSize=10,
            connectTimeoutMS=2000,
            serverSelectionTimeoutMS=3000
        )
        self.db = self.client.mapdb
        self.collection = self.db.map_elements

    def get_latest_elements(self, tile):
        selected_timestamp = random.choice(self.timestamps)
        
        pipeline = [
            {
                "$match": {
                    "tile": tile,
                    "tsver": {"$lte": selected_timestamp}
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
            }
        ]

        return list(self.collection.aggregate(
            pipeline,
            allowDiskUse=True
        ))

    def close(self):
        self.client.close()

class QueryWorker(threading.Thread):
    def __init__(self, worker_id: int, duration: int, results: List[Dict], requests_per_thread: int = 5):
        super().__init__()
        self.worker_id = worker_id
        self.duration = duration
        self.results = results
        self.requests_per_thread = requests_per_thread
        self.stop_flag = False
        self.db = MapElementDB()

    def run(self):
        start_time = time.time()
        while not self.stop_flag and (time.time() - start_time) < self.duration:
            for _ in range(self.requests_per_thread):
                tile_num = str(random.randint(0, 1999)).zfill(4)
                tile = f"tile_{tile_num}"
                
                query_start = time.time()
                try:
                    results = self.db.get_latest_elements(tile)
                    query_time = time.time() - query_start
                    
                    self.results.append({
                        'worker_id': self.worker_id,
                        'tile': tile,
                        'query_time': query_time,
                        'elements_count': len(results),
                        'success': True,
                        'timestamp': time.time()
                    })
                except Exception as e:
                    self.results.append({
                        'worker_id': self.worker_id,
                        'tile': tile,
                        'query_time': time.time() - query_start,
                        'elements_count': 0,
                        'success': False,
                        'error': str(e)
                    })
            
            # Small sleep to prevent overwhelming the server
            time.sleep(0.1)

    def stop(self):
        self.stop_flag = True
        self.db.close()

def run_benchmark(num_clients: int, duration: int) -> Dict:
    results = []
    workers = []

    print(f"Starting {num_clients} workers...")
    for i in range(num_clients):
        worker = QueryWorker(i, duration, results)
        workers.append(worker)
        worker.start()
        # Add small delay between starting workers
        if i > 0 and i % 10 == 0:
            time.sleep(0.1)

    start_time = time.time()
    while time.time() - start_time < duration:
        current_queries = len(results)
        elapsed = time.time() - start_time
        qps = current_queries / elapsed if elapsed > 0 else 0
        print(f"Progress: {elapsed:.1f}s, Queries: {current_queries}, QPS: {qps:.1f}", end='\r')
        time.sleep(1)

    print("\nStopping workers...")
    for worker in workers:
        worker.stop()
    for worker in workers:
        worker.join()

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

    return {
        'num_clients': num_clients,
        'duration': duration,
        'total_queries': len(results),
        'successful_queries': successful_queries,
        'errors': len(results) - successful_queries,
        'qps': len(results) / duration,
        'avg_time': statistics.mean(query_times) if query_times else 0,
        'median_time': statistics.median(query_times) if query_times else 0,
        'min_time': min(query_times) if query_times else 0,
        'max_time': max(query_times) if query_times else 0,
        'total_elements': total_elements
    }

def main():
    # Reduced client counts for testing
    #client_counts = [10, 50, 100, 200]  # Start with smaller numbers first
    client_counts = [100, 500, 1000, 5000, 10000]  # Start with smaller numbers first
    duration = 30  # Reduced duration for testing
    cooldown = 10

    results_dir = "benchmark_results"
    os.makedirs(results_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = os.path.join(results_dir, f"benchmark_results_{timestamp}.txt")

    with open(results_file, 'w') as f:
        print("\nDocumentDB Query Performance Benchmark")
        print(f"Results will be saved to: {results_file}")

        results = []
        for num_clients in client_counts:
            print(f"\nTesting with {num_clients} concurrent clients...")
            result = run_benchmark(num_clients, duration)
            results.append(result)

            summary = (f"\nResults for {num_clients} clients:\n"
                      f"QPS: {result['qps']:.1f}, "
                      f"Avg: {result['avg_time']*1000:.1f}ms, "
                      f"Success: {(result['successful_queries']/result['total_queries']*100):.1f}%")
            print(summary)
            f.write(f"{summary}\n")

            print(f"Cooling down for {cooldown} seconds...")
            time.sleep(cooldown)

        # Write final summary
        print("\nBenchmark Summary")
        print("Clients | QPS     | Avg(ms) | Success %")
        print("-" * 40)
        for r in results:
            success_rate = (r['successful_queries'] / r['total_queries'] * 100) if r['total_queries'] > 0 else 0
            print(f"{r['num_clients']:7d} | {r['qps']:7.1f} | {r['avg_time']*1000:7.1f} | {success_rate:8.1f}%")

if __name__ == "__main__":
    main()
