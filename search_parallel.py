import json
from elasticsearch import Elasticsearch
import sys
from multiprocessing import Pool
from tqdm import tqdm

index = "wiki"
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

def process_shard(shard_id, lines, index_name):
    output_file_path = f"output/output_{shard_id}.json"
    
    with open(output_file_path, 'w') as output_file:
        with tqdm(total=len(lines), desc=f"Shard {shard_id}", unit="line", position=shard_id-1) as pbar:
            for line in lines:
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
                pbar.update(1)

def parallel_process_json_file(file_path, index_name, num_shards):
    # Initialize Elasticsearch client
    check_es_connection(es)

    with open(file_path, 'r') as file:
        lines = file.readlines()

    # Split the lines into N shards
    shard_size = len(lines) // num_shards
    shards = [lines[i:i+shard_size] for i in range(0, len(lines), shard_size)]

    # Create a pool of processes
    pool = Pool(processes=num_shards)

    # Assign each shard to a separate process
    args = [(i+1, shard, index_name) for i, shard in enumerate(shards)]
    pool.starmap(process_shard, args)

    # Close the pool
    pool.close()
    pool.join()

# Example usage
num_shards = int(sys.argv[2])
parallel_process_json_file(sys.argv[1], index, num_shards)