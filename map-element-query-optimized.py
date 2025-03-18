from pymongo import MongoClient, ASCENDING, DESCENDING
import random
import time
import logging

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

def main():
    try:
        start_time = time.time()
        map_db = MapElementDB()
        
        tile = "tile_0001"
        max_timestamp = map_db.get_random_timestamp(tile)
        
        if max_timestamp is None:
            print(f"No data found for tile {tile}")
            return
            
        print(f"\nQuerying tile: {tile}")
        
        results = map_db.get_latest_elements(tile, max_timestamp)
        
        # Print sample results
        print(f"\nSample Results (first 5 of {len(results)} elements):")
        print("-" * 50)
        for result in results[:5]:
            print(f"Element: {result['element']}")
            print(f"Latest Version (tsver): {result['max_tsver']}")
            print("-" * 50)

        # Print summary
        execution_time = time.time() - start_time
        print("\nSummary:")
        print("=" * 50)
        print(f"Total elements found: {len(results)}")
        print(f"Execution time: {execution_time:.3f} seconds")
        print("=" * 50)

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        map_db.close()

if __name__ == "__main__":
    main()
