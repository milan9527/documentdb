from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime
import random
import string

class MapElementDB:
    def __init__(self, db_name="mapdb", collection_name="map_elements"):
        # Amazon DocumentDB connection settings
        username = "milan"
        password = "xxxx"
        host = "ping5.cluster-c7b8fns5un9o.us-east-1.docdb.amazonaws.com"
        port = 27017

        # Connection string without SSL/TLS
        connection_string = f"mongodb://{username}:{password}@{host}:{port}/?tls=false&retryWrites=false"

        # Connect to DocumentDB without SSL
        self.client = MongoClient(connection_string)
        
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        
        # Create indexes
        self.create_indexes()

    def create_indexes(self):
        # Create compound index for efficient querying
        self.collection.create_index([
            ("tile", ASCENDING),
            ("tsver", DESCENDING),
            ("element", ASCENDING)
        ])

    def insert_sample_data(self):
        # Generate sample data
        branch = "main"
        tiles = [f"tile_{i:04d}" for i in range(2000)]
        elements = [f"elem_{i:04d}" for i in range(10000)]
        
        bulk_data = []
        for tile in tiles:
            for element in elements:
                # Generate 1-200 versions for each element
                num_versions = random.randint(1, 200)
                for _ in range(num_versions):
                    doc = {
                        "branch": branch,
                        "tile": tile,
                        "element": element,
                        "tsver": int(datetime.now().timestamp() * 1000) - random.randint(0, 365*24*60*60*1000),
                        "element_value": ''.join(random.choices(string.ascii_letters + string.digits, k=20)),
                        "element_md5": ''.join(random.choices(string.hexdigits, k=16))
                    }
                    bulk_data.append(doc)
                
                if len(bulk_data) >= 1000:
                    self.collection.insert_many(bulk_data)
                    bulk_data = []
        
        if bulk_data:
            self.collection.insert_many(bulk_data)

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
        """
        Close the database connection
        """
        self.client.close()

def main():
    try:
        # Initialize database
        map_db = MapElementDB()
        
        # Insert sample data first
        print("Inserting sample data...")
        map_db.insert_sample_data()
        print("Sample data insertion completed!")

        # Example query
        tile = "tile_0001"
        max_timestamp = int(datetime.now().timestamp() * 1000)  # current time in milliseconds
        
        print(f"\nQuerying latest elements for tile {tile}...")
        results = map_db.get_latest_elements(tile, max_timestamp)
        
        # Print first few results
        print(f"\nFound {len(results)} elements. Showing first 5:")
        for result in results[:5]:
            print(f"Element: {result['element']}")
            print(f"Latest Version Timestamp: {result['max_tsver']}")
            print(f"Value: {result['element_value']}")
            print(f"MD5: {result['element_md5']}")
            print("---")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    
    finally:
        # Close the connection
        map_db.close()

if __name__ == "__main__":
    main()
