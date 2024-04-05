from elasticsearch import Elasticsearch, helpers
import json
import sys
# Replace with the path to your actual JSON file
json_file_path = sys.argv[1]

def generate_actions(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            source_data = json.loads(line.strip())
            action = {
                '_op_type': 'index',  # Operation type for the bulk API
                '_index': "test",  # Index name
                '_id': source_data['id'],  # Document ID
                '_source': {
                    'text': source_data['text']  # Keeping only the 'text' field
                }
            }
            print(action)
            break

# Main function to process the JSON file and index the data
def main():
    # Verify Elasticsearch connection
    generate_actions(json_file_path)

# Run the main function
if __name__ == "__main__":
    main()
