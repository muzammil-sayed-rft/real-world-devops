# working bulk migrate 100GB++ data from atlas to eks in parralle
# IMP enable swap 

import pymongo
from lz4.frame import compress, decompress
import os
import time
import logging
from multiprocessing import Pool

# Set up logging
logging.basicConfig(filename='migration.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Source MongoDB connection settings
source_uri = 'mongodb+srv://username:password@dsapay-uat.uvhrp.mongodb.net/'

# Destination MongoDB connection settings
destination_host = 'your-mongo-eks-host'
destination_port= 27017
destination_username = 'root'
destination_password = 'zoot123'

# Number of parallel processes (using 2/3 of available CPU cores)
num_processes = os.cpu_count() // 2

# Output directory for compressed data
output_directory = 'compressed_data'

def migrate_collection(collection_info):
    source_client = pymongo.MongoClient(source_uri)
    destination_client = pymongo.MongoClient(destination_host, destination_port, username=destination_username, password=destination_password)

    source_db_name, collection_name = collection_info

    try:
        # Get source collection
        source_db = source_client[source_db_name]
        source_collection = source_db[collection_name]

        # Measure start time
        start_time = time.time()

        # Stream data in chunks
        chunk_size = 1000
        num_documents = source_collection.count_documents({})
        for i in range(0, num_documents, chunk_size):
            documents = list(source_collection.find().skip(i).limit(chunk_size))

            if documents:
                # Compress data
                compressed_data = compress(str(documents).encode())

                # Save compressed data to file
                output_file = os.path.join(output_directory, f"{source_db_name}_{collection_name}_{i}.lz4")
                with open(output_file, 'wb') as f:
                    f.write(compressed_data)

                # Create destination collection
                destination_db = destination_client[source_db_name]
                destination_collection = destination_db[collection_name]

                # Insert documents into destination collection
                destination_collection.insert_many(documents)

                logging.info(f"Processed chunk {i}/{num_documents}")

        # Measure end time
        end_time = time.time()

        # Calculate time taken
        time_taken = end_time - start_time

        logging.info(f"Collection '{collection_name}' migrated to database '{source_db_name}' in {time_taken:.2f} seconds.")

    finally:
        source_client.close()
        destination_client.close()

if __name__ == "__main__":
    # Connect to source MongoDB
    source_client = pymongo.MongoClient(source_uri)

    # Get list of databases from source
    source_databases = source_client.list_database_names()

    # Create output directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    # Get list of collections in each database
    collections = []
    for db_name in source_databases:
        source_db = source_client[db_name]
        db_collections = source_db.list_collection_names()
        collections.extend([(db_name, collection) for collection in db_collections])

    # Migrate collections in parallel
    with Pool(processes=num_processes) as pool:
        pool.map(migrate_collection, collections)

    source_client.close()