from pymongo import MongoClient, ASCENDING, DESCENDING
import concurrent.futures
import threading
import random
import time
import logging
import statistics
from datetime import datetime
from typing import List, Dict
from datetime import datetime
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MapElementDB:
    def __init__(self, db_name: str = "mapdb", collection_name: str = "map_elements"):
        username = "milan"
        password = "wAr16dk7"
        host = "ping5.cluster-c7b8fns5un9o.us-east-1.docdb.amazonaws.com"
        port = 27017

        # Define the timestamp array
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

        connection_string = f"mongodb://{username}:{password}@{host}:{port}/?tls=false&retryWrites=false"
        self.client = MongoClient(connection_string)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Create necessary indexes for optimized queries"""
        try:
            self.collection.create_index([("tile", 1), ("tsver", -1)])
            self.collection.create_index([("tile", 1), ("element", 1), ("tsver", -1)])
        except Exception as e:
            logger.warning(f"Index creation failed: {str(e)}")

    def get_random_timestamp(self, tile):
        """Get a random timestamp from existing data for a specific tile"""
        result = self.collection.find_one(
            {"tile": tile},
            {"tsver": 1, "_id": 0}
        )
        return result["tsver"] if result else None

    def get_latest_elements(self, tile, max_timestamp):
        """Get the latest version of each element"""
        # Randomly select a timestamp from the array
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

    def close(self):
        self.client.close()

class QueryClient(threading.Thread):
    """Client thread that continuously performs queries"""
    def __init__(self, client_id: int, duration: int, results: List[Dict]):
        super().__init__()
        self.client_id = client_id
        self.duration = duration
        self.results = results
        self.db_connection = MapElementDB()
        self.stop_flag = False

    def run(self):
        try:
            start_time = time.time()
            while not self.stop_flag and (time.time() - start_time) < self.duration:
                # Generate random tile
                tile_num = str(random.randint(0, 1999)).zfill(4)
                tile = f"tile_{tile_num}"
                
                query_start = time.time()
                try:
                    results = self.db_connection.get_latest_elements(tile, None)
                    query_time = time.time() - query_start
                    
                    self.results.append({
                        'client_id': self.client_id,
                        'tile': tile,
                        'query_time': query_time,
                        'elements_count': len(results),
                        'success': True,
                        'timestamp': time.time()
                    })
                except Exception as e:
                    logger.error(f"Client {self.client_id} query error: {str(e)}")
                    self.results.append({
                        'client_id': self.client_id,
                        'tile': tile,
                        'query_time': time.time() - query_start,
                        'elements_count': 0,
                        'success': False,
                        'timestamp': time.time(),
                        'error': str(e)
                    })
        finally:
            self.db_connection.close()

    def stop(self):
        self.stop_flag = True

def run_benchmark(num_clients: int, duration: int) -> Dict:
    """
    Run benchmark with specified number of clients for given duration
    
    Args:
        num_clients: Number of concurrent clients
        duration: Test duration in seconds
    """
    results = []
    clients = []

    # Start clients
    logger.info(f"Starting {num_clients} clients for {duration} seconds...")
    for i in range(num_clients):
        client = QueryClient(i, duration, results)
        clients.append(client)
        client.start()

    # Monitor progress
    start_time = time.time()
    while time.time() - start_time < duration:
        time.sleep(1)
        queries_so_far = len(results)
        elapsed = time.time() - start_time
        qps = queries_so_far / elapsed if elapsed > 0 else 0
        logger.info(f"Progress: {elapsed:.1f}s, Queries: {queries_so_far}, QPS: {qps:.1f}")

    # Stop clients
    for client in clients:
        client.stop()

    # Wait for clients to finish
    for client in clients:
        client.join()

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

# Add this at the beginning with other imports
from datetime import datetime
import os

# Modify the main() function to include file output:
def main():
    # Test configuration
    client_counts = [100, 500, 1000, 5000, 10000, 40000]
    duration = 60  # seconds per test
    cooldown = 10  # seconds between tests

    # Create results directory if it doesn't exist
    results_dir = "benchmark_results"
    os.makedirs(results_dir, exist_ok=True)

    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = os.path.join(results_dir, f"benchmark_results_{timestamp}.txt")

    # Open file for writing results
    with open(results_file, 'w') as f:
        # Write header information
        f.write("DocumentDB Query Performance Benchmark\n")
        f.write("=" * 80 + "\n")
        f.write(f"Start time: {datetime.now()}\n")
        f.write(f"Test duration per concurrency level: {duration} seconds\n")
        f.write(f"Testing random tiles from tile_0000 to tile_1999\n")
        f.write("=" * 80 + "\n\n")

        print("\nDocumentDB Query Performance Benchmark")
        print("=" * 80)
        print(f"Start time: {datetime.now()}")
        print(f"Test duration per concurrency level: {duration} seconds")
        print(f"Testing random tiles from tile_0000 to tile_1999")
        print("=" * 80)

        results = []
        for num_clients in client_counts:
            print(f"\nTesting with {num_clients} concurrent clients...")
            f.write(f"\nTesting with {num_clients} concurrent clients...\n")
            
            result = run_benchmark(num_clients, duration)
            results.append(result)

            # Write detailed results to file
            f.write(f"\nResults for {num_clients} clients:\n")
            f.write("-" * 60 + "\n")
            f.write(f"Total queries: {result['total_queries']}\n")
            f.write(f"Successful queries: {result['successful_queries']}\n")
            f.write(f"Errors: {result['errors']}\n")
            f.write(f"Queries per second: {result['qps']:.2f}\n")
            f.write(f"Average query time: {result['avg_time']*1000:.2f} ms\n")
            f.write(f"Median query time: {result['median_time']*1000:.2f} ms\n")
            f.write(f"Min query time: {result['min_time']*1000:.2f} ms\n")
            f.write(f"Max query time: {result['max_time']*1000:.2f} ms\n")
            f.write(f"Total elements retrieved: {result['total_elements']}\n")

            # Print to console as well
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
            f.write(f"\nCooling down for {cooldown} seconds...\n")
            time.sleep(cooldown)

        # Write summary table to file
        f.write("\nBenchmark Summary\n")
        f.write("=" * 100 + "\n")
        f.write("Clients | QPS     | Avg(ms) | Median(ms) | Min(ms) | Max(ms) | Success % | Total Queries\n")
        f.write("-" * 100 + "\n")

        # Print summary table to console
        print("\nBenchmark Summary")
        print("=" * 100)
        print("Clients | QPS     | Avg(ms) | Median(ms) | Min(ms) | Max(ms) | Success % | Total Queries")
        print("-" * 100)

        for r in results:
            success_rate = (r['successful_queries'] / r['total_queries'] * 100) if r['total_queries'] > 0 else 0
            summary_line = (f"{r['num_clients']:7d} | {r['qps']:7.1f} | {r['avg_time']*1000:7.1f} | "
                          f"{r['median_time']*1000:9.1f} | {r['min_time']*1000:7.1f} | {r['max_time']*1000:7.1f} | "
                          f"{success_rate:8.1f}% | {r['total_queries']:13d}")
            
            # Write to file and print to console
            f.write(summary_line + "\n")
            print(summary_line)

        print(f"\nResults have been saved to: {results_file}")

if __name__ == "__main__":
    main()
