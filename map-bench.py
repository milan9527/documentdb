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
from multiprocessing import Process, Queue

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logging.getLogger('pymongo.topology').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

class MapElementDB:
    def __init__(self, db_name: str = "mapdb", collection_name: str = "map_elements"):
        username = "milan"
        password = "xxxx"
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

        connection_string = f"mongodb://{username}:{password}@{host}:{port}/?tls=false&retryWrites=false&readPreference=secondaryPreferred"
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

class QueryWorker(threading.Thread):
    def __init__(self, worker_id: int, duration: int, results_queue: Queue, requests_per_thread: int = 3):
        super().__init__()
        self.worker_id = worker_id
        self.duration = duration
        self.results_queue = results_queue
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
                    results = self.db.get_latest_elements(tile, None)
                    query_time = time.time() - query_start
                    
                    self.results_queue.put({
                        'worker_id': self.worker_id,
                        'tile': tile,
                        'query_time': query_time,
                        'elements_count': len(results),
                        'success': True,
                        'timestamp': time.time()
                    })
                except Exception as e:
                    self.results_queue.put({
                        'worker_id': self.worker_id,
                        'tile': tile,
                        'query_time': time.time() - query_start,
                        'elements_count': 0,
                        'success': False,
                        'error': str(e)
                    })
            
            time.sleep(0.2)  # Increased delay to prevent overwhelming

    def stop(self):
        self.stop_flag = True
        self.db.close()

def process_worker(process_id: int, num_threads: int, duration: int, results_queue: Queue):
    workers = []
    logger.info(f"Process {process_id} starting with {num_threads} threads")
    
    try:
        for i in range(num_threads):
            worker = QueryWorker(
                worker_id=process_id * 100 + i,
                duration=duration,
                results_queue=results_queue
            )
            workers.append(worker)
            worker.start()

        # Wait for duration
        end_time = time.time() + duration
        while time.time() < end_time:
            time.sleep(1)

        # Stop all workers
        for worker in workers:
            worker.stop()

        # Join with timeout
        for worker in workers:
            worker.join(timeout=5)  # 5 second timeout for each thread
            
    except Exception as e:
        logger.error(f"Error in process {process_id}: {str(e)}")
    finally:
        # Force stop any remaining workers
        for worker in workers:
            if worker.is_alive():
                worker.stop()
        logger.info(f"Process {process_id} finished")

def run_benchmark(total_clients: int, duration: int) -> Dict:
    """
    Run benchmark using processes and threads with optimized settings for high concurrency
    """
    if total_clients > 1000:
        num_processes = 32
    else:
        num_processes = min(os.cpu_count() * 2 or 8, max(4, total_clients // 50))
    
    threads_per_process = max(1, total_clients // num_processes)
    
    results_queue = Queue()
    processes = []

    logger.info(f"Starting benchmark with {num_processes} processes and {threads_per_process} threads per process "
                f"(Total clients: {num_processes * threads_per_process})")
    
    # Start processes
    for i in range(num_processes):
        p = Process(target=process_worker, 
                   args=(i, threads_per_process, duration, results_queue))
        p.daemon = True  # Make processes daemon so they don't block program exit
        processes.append(p)
        p.start()
        time.sleep(0.2)

    # Monitor progress
    start_time = time.time()
    results = []
    
    while time.time() - start_time < duration + 10:  # Extra 10 seconds for cleanup
        try:
            # Collect results
            while not results_queue.empty():
                try:
                    results.append(results_queue.get_nowait())
                except:
                    break

            # Progress logging
            elapsed = time.time() - start_time
            current_queries = len(results)
            qps = current_queries / elapsed if elapsed > 0 else 0
            logger.info(f"Progress: {elapsed:.1f}s, Queries: {current_queries}, QPS: {qps:.1f}")
            
            # Check if all processes are done
            alive_processes = [p for p in processes if p.is_alive()]
            if not alive_processes and elapsed > duration:
                break
                
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error in monitoring: {str(e)}")

    # Cleanup
    logger.info("Cleaning up processes...")
    for p in processes:
        if p.is_alive():
            logger.info(f"Terminating process {p.pid}")
            p.terminate()
            time.sleep(0.1)
            if p.is_alive():
                logger.info(f"Killing process {p.pid}")
                p.kill()
    
    # Wait for processes with timeout
    for p in processes:
        p.join(timeout=5)

    # Process results
    if not results:
        return {
            'total_clients': total_clients,
            'num_processes': num_processes,
            'threads_per_process': threads_per_process,
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
        'total_clients': total_clients,
        'num_processes': num_processes,
        'threads_per_process': threads_per_process,
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
    # Updated test configuration with smaller numbers
    client_counts = [500, 1000, 5000]
    duration = 300  # Reduced duration
    cooldown = 15  # increased cooldown between tests

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
        f.write(f"Start Time: {timestamp}\n\n")

        # Run benchmarks for each client count
        for client_count in client_counts:
            logger.info(f"\nStarting benchmark with {client_count} clients...")
            f.write(f"\nBenchmark with {client_count} clients\n")
            f.write("-" * 40 + "\n")

            try:
                results = run_benchmark(client_count, duration)
                
                # Write results to file
                f.write(f"Configuration:\n")
                f.write(f"- Total Clients: {results['total_clients']}\n")
                f.write(f"- Processes: {results['num_processes']}\n")
                f.write(f"- Threads per Process: {results['threads_per_process']}\n")
                f.write(f"- Duration: {results['duration']} seconds\n\n")
                
                f.write(f"Results:\n")
                f.write(f"- Total Queries: {results['total_queries']}\n")
                f.write(f"- Successful Queries: {results['successful_queries']}\n")
                f.write(f"- Errors: {results['errors']}\n")
                f.write(f"- Queries per Second: {results['qps']:.2f}\n")
                f.write(f"- Average Query Time: {results['avg_time']*1000:.2f} ms\n")
                f.write(f"- Median Query Time: {results['median_time']*1000:.2f} ms\n")
                f.write(f"- Min Query Time: {results['min_time']*1000:.2f} ms\n")
                f.write(f"- Max Query Time: {results['max_time']*1000:.2f} ms\n")
                f.write(f"- Total Elements Retrieved: {results['total_elements']}\n\n")

            except Exception as e:
                error_msg = f"Error during benchmark with {client_count} clients: {str(e)}"
                logger.error(error_msg)
                f.write(f"ERROR: {error_msg}\n\n")

            # Cooldown period between tests
            logger.info(f"Cooling down for {cooldown} seconds...")
            time.sleep(cooldown)

        # Write completion time
        end_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"\nBenchmark completed at: {end_timestamp}\n")

    logger.info(f"\nBenchmark complete. Results written to {results_file}")

if __name__ == "__main__":
    main()

