from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import random
import time
from typing import Optional, List, Dict, Any
import logging
from contextlib import contextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
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

    def get_random_timestamp(self, tile):
        """
        Get a random timestamp from existing data for a specific tile
        """
        pipeline = [
            {
                "$match": {
                    "tile": tile
                }
            },
            {
                "$group": {
                    "_id": None,
                    "timestamps": {"$push": "$tsver"}
                }
            }
        ]
        result = list(self.collection.aggregate(pipeline))
        if result and result[0]["timestamps"]:
            return random.choice(result[0]["timestamps"])
        return None

    def get_latest_elements(self, tile, max_timestamp):
        """
        Get the latest version of each element in a specified tile and time range
        """
        pipeline = [
            {
                "$match": {
                    "tile": tile,
                    "tsver": {"$lte": max_timestamp}
                }
            },
            {
                "$sort": {"tsver": -1}
            },
            {
                "$group": {
                    "_id": "$element",
                    "max_tsver": {"$first": "$tsver"},
                    "element_value": {"$first": "$element_value"},
                    "element_md5": {"$first": "$element_md5"}
                }
            },
            {
                "$project": {
                    "element": "$_id",
                    "max_tsver": 1,
                    "element_value": 1,
                    "element_md5": 1,
                    "_id": 0
                }
            }
        ]
        
        return list(self.collection.aggregate(pipeline))

    def close(self):
        self.client.close()

def main():
    try:
        start_time = time.time()
        map_db = MapElementDB()
        
        # Query parameters
        tile = "tile_0001"
        
        # Get a random timestamp from existing data
        max_timestamp = map_db.get_random_timestamp(tile)
        if max_timestamp is None:
            print(f"No data found for tile {tile}")
            return
            
        print("\nQuery Parameters:")
        print(f"Tile: {tile}")
        print(f"Cutoff timestamp: {datetime.fromtimestamp(max_timestamp/1000)}")
        
        results = map_db.get_latest_elements(tile, max_timestamp)
        
        # Print detailed results (first 5)
        print(f"\nSample Results (first 5 of {len(results)} elements):")
        print("-" * 50)
        for result in results[:5]:
            print(f"Element: {result['element']}")
            print(f"Latest Version: {datetime.fromtimestamp(result['max_tsver']/1000)}")
            print(f"Value: {result['element_value']}")
            print(f"MD5: {result['element_md5']}")
            print("-" * 50)

        # Print summary
        end_time = time.time()
        execution_time = end_time - start_time
        
        print("\nSummary:")
        print("=" * 50)
        print(f"Tile: {tile}")
        print(f"Total elements found: {len(results)}")
        print(f"Execution time: {execution_time:.3f} seconds")
        print("=" * 50)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        map_db.close()

if __name__ == "__main__":
    main()
