from elasticsearch import Elasticsearch, helpers
import json
import sys


# Replace with the actual index name you want to use in Elasticsearch
index_name = 'wiki'

# Connect to your Elasticsearch cluster
es = Elasticsearch("http://localhost:9200")

# Function to delete all documents from the index
def delete_all_documents():
    query = {
        "query": {
            "match_all": {}
        }
    }
    es.delete_by_query(index=index_name, body=query)
    print(f"All documents deleted from index: {index_name}")

def check_es_connection(es_client):
    try:
        if not es_client.ping():
            raise ValueError("Connection to Elasticsearch cluster failed!")
        print("Successfully connected to Elasticsearch.")
    except Exception as e:
        raise ValueError(f"Error connecting to Elasticsearch: {e}")

# Main function to process the JSON file and index the data
def main():
    # Verify Elasticsearch connection
    check_es_connection(es)

    # Delete all existing documents from the index
    delete_all_documents()

# Run the main function
if __name__ == "__main__":
    main()