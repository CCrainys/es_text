from elasticsearch import Elasticsearch

# Replace with the actual index name you want to use in Elasticsearch
index_name = 'wiki'

# Connect to your Elasticsearch cluster
es = Elasticsearch("http://localhost:9200")

def check_es_connection(es_client):
    try:
        if not es_client.ping():
            raise ValueError("Connection to Elasticsearch cluster failed!")
        print("Successfully connected to Elasticsearch.")
    except Exception as e:
        raise ValueError(f"Error connecting to Elasticsearch: {e}")

# Function to get the document count in the index
def get_document_count():
    count = es.count(index=index_name)
    print(f"Number of documents in index '{index_name}': {count['count']}")

# Main function to process the JSON file and index the data
def main():
    # Verify Elasticsearch connection
    check_es_connection(es)

    # Get the document count before indexing
    get_document_count()

# Run the main function
if __name__ == "__main__":
    main()