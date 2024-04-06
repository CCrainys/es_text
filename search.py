import json
from elasticsearch import Elasticsearch
import sys

index="wiki"

es = Elasticsearch("http://localhost:9200")

# Function to check if the connection to Elasticsearch is successful
def check_es_connection(es_client):
    try:
        if not es_client.ping():
            raise ValueError("Connection to Elasticsearch cluster failed!")
        print("Successfully connected to Elasticsearch.")
    except Exception as e:
        raise ValueError(f"Error connecting to Elasticsearch: {e}")

def ngram(sentence, n=8):
    """
    Generates n-grams from a given sentence.
    
    Args:
        sentence (str): The input sentence.
        n (int, optional): The value of n for n-grams. Default is 8.
        
    Returns:
        list: A list of n-grams.
    """
    
    # Split the cleaned sentence into words
    words = sentence.split()
    
    # Generate n-grams
    ngrams = [' '.join(words[i:i+n]) for i in range(len(words)-n+1)]
    
    return ngrams

def process_json_file(file_path, index_name, output_file_path):
    # Initialize Elasticsearch client
    check_es_connection(es)

    with open(file_path, 'r') as file, open(output_file_path, 'w') as output_file:
        for line in file:
            # Load the line as a JSON object
            data = json.loads(line.strip())
            
            # Get the text from the JSON object
            text = data.get('text', '')
            
            # Convert the text into n-grams
            ngrams = ngram(text)
            
            # Perform exact match with n-grams
            result = [int(es.count(index=index_name, body={
                'query': {
                    'match_phrase': {
                        'text': ngram
                    }
                }
            })['count'] > 0) for ngram in ngrams]
            
            # Write the result list to the output file
            output_file.write(json.dumps(result) + '\n')

# Example usage
process_json_file(sys.argv[1], index, 'output.json')