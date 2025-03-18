from pymongo import MongoClient, ASCENDING, DESCENDING
import concurrent.futures
import threading
import random
import time
import logging
import statistics
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MapElementDB:
    def __init__(self, db_name: str = "mapdb", collection_name: str = "map_elements"):
        username = "milan"
        password = "xxxx"
        host = "ping5.cluster-c7b8fns5un9o.us-east-1.docdb.amazonaws.com"
        port = 27017

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
        pipeline = [
            {
                "$match": {
                    "tile": tile,
                    #"tsver": {"$lte": max_timestamp}
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
            hint={"tile": 1, "tsver": -1}  # Corrected hint format
        ))

    def close(self):
        self.client.close()

def perform_query(map_db, tile):
    """Execute a single query and return its execution time"""
    try:
        start_time = time.time()
        results = map_db.get_latest_elements(tile, None)  # Using fixed timestamp from the query
        execution_time = time.time() - start_time
        return execution_time, len(results)
    except Exception as e:
        logger.error(f"Query error: {str(e)}")
        return None

def benchmark_concurrency(concurrency, num_queries, tile):
    """Benchmark queries with specific concurrency level"""
    execution_times = []
    total_elements = 0
    successful_queries = 0
    errors = 0

    # Create connection pool
    connections = [MapElementDB() for _ in range(concurrency)]
    
    def worker(conn):
        try:
            result = perform_query(conn, tile)
            if result:
                exec_time, num_elements = result
                return exec_time, num_elements, True
            return 0, 0, False
        except Exception as e:
            logger.error(f"Worker error: {str(e)}")
            return 0, 0, False

    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = []
        for i in range(num_queries):
            conn = connections[i % concurrency]
            futures.append(executor.submit(worker, conn))

        for future in concurrent.futures.as_completed(futures):
            exec_time, num_elements, success = future.result()
            if success:
                execution_times.append(exec_time)
                total_elements += num_elements
                successful_queries += 1
            else:
                errors += 1

    total_time = time.time() - start_time

    # Close all connections
    for conn in connections:
        conn.close()

    return {
        'concurrency': concurrency,
        'total_queries': num_queries,
        'successful_queries': successful_queries,
        'errors': errors,
        'total_time': total_time,
        'avg_time': statistics.mean(execution_times) if execution_times else 0,
        'median_time': statistics.median(execution_times) if execution_times else 0,
        'min_time': min(execution_times) if execution_times else 0,
        'max_time': max(execution_times) if execution_times else 0,
        'queries_per_second': successful_queries / total_time if total_time > 0 else 0,
        'total_elements': total_elements
    }

def main():
    tile = "tile_0001"
    num_queries = 100  # Number of queries per concurrency level
    concurrency_levels = [100, 500, 1000, 5000, 10000, 40000]

    print("\nDocumentDB Query Performance Benchmark")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")
    print(f"Target tile: {tile}")
    print(f"Queries per concurrency level: {num_queries}")
    print("=" * 80)

    results = []
    for concurrency in concurrency_levels:
        logger.info(f"\nTesting concurrency level: {concurrency}")
        result = benchmark_concurrency(concurrency, num_queries, tile)
        results.append(result)
        
        print(f"\nResults for concurrency {concurrency}:")
        print("-" * 60)
        print(f"Total queries: {result['total_queries']}")
        print(f"Successful queries: {result['successful_queries']}")
        print(f"Errors: {result['errors']}")
        print(f"Total time: {result['total_time']:.2f} seconds")
        print(f"Average query time: {result['avg_time']*1000:.2f} ms")
        print(f"Median query time: {result['median_time']*1000:.2f} ms")
        print(f"Min query time: {result['min_time']*1000:.2f} ms")
        print(f"Max query time: {result['max_time']*1000:.2f} ms")
        print(f"Queries per second: {result['queries_per_second']:.2f}")
        print(f"Total elements retrieved: {result['total_elements']}")
        
        # Add a delay between tests
        time.sleep(5)

    print("\nBenchmark Summary")
    print("=" * 80)
    print("Concurrency | QPS    | Avg(ms) | Median(ms) | Min(ms) | Max(ms) | Success Rate")
    print("-" * 80)
    for r in results:
        success_rate = (r['successful_queries'] / r['total_queries']) * 100
        print(f"{r['concurrency']:11d} | {r['queries_per_second']:6.1f} | {r['avg_time']*1000:7.1f} | "
              f"{r['median_time']*1000:9.1f} | {r['min_time']*1000:7.1f} | {r['max_time']*1000:7.1f} | {success_rate:11.1f}%")

if __name__ == "__main__":
    main()
