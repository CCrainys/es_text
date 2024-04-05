from elasticsearch import Elasticsearch, helpers
import json
import sys
# Replace with the path to your actual JSON file
json_file_path = sys.argv[1]

# Replace with the actual index name you want to use in Elasticsearch
index_name = 'wiki'

# Connect to your Elasticsearch cluster
es = Elasticsearch("http://localhost:9200")

# Function to check if the connection to Elasticsearch is successful
def check_es_connection(es_client):
    try:
        if not es_client.ping():
            raise ValueError("Connection to Elasticsearch cluster failed!")
        print("Successfully connected to Elasticsearch.")
    except Exception as e:
        raise ValueError(f"Error connecting to Elasticsearch: {e}")

# Function to generate actions for the bulk API
def generate_actions(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            source_data = json.loads(line.strip())
            action = {
                '_op_type': 'index',  # Operation type for the bulk API
                '_index': index_name,  # Index name
                '_id': source_data['id'],  # Document ID
                '_source': {
                    'text': source_data['text']  # Keeping only the 'text' field
                }
            }
            yield action

# Main function to process the JSON file and index the data
def main():
    # Verify Elasticsearch connection
    check_es_connection(es)
    
    # Process the file and index the data with the bulk API
    try:
        success, failed = helpers.bulk(es, generate_actions(json_file_path))
        print(f"Successfully indexed {success} documents.")
        if failed:
            print(f"Failed to index {failed} documents.")
    except Exception as e:
        print(f"Error indexing documents: {e}")

# Run the main function
if __name__ == "__main__":
    main()
