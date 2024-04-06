import re

def ngram(sentence, n=8):
    """
    Generates n-grams from a given sentence.
    
    Args:
        sentence (str): The input sentence.
        n (int, optional): The value of n for n-grams. Default is 8.
        
    Returns:
        list: A list of n-grams.
    """
    # Convert the sentence to lowercase and remove non-alphanumeric characters
    # cleaned_sentence = re.sub(r'[^a-zA-Z0-9\s]', '', sentence.lower())
    
    # Split the cleaned sentence into words
    words = sentence.split()
    
    # Generate n-grams
    ngrams = [' '.join(words[i:i+n]) for i in range(len(words)-n+1)]
    
    return ngrams

# Example usage
sentence = "The quick brown fox jumps over the lazy dog."
eight_grams = ngram(sentence)
print(eight_grams)