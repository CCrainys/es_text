from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200")

query = {
    "query": {
        "match": {
            "text": "organic chemistry"
        }
    }
}

response = es.search(index="wiki", body=query)
print("Total hits:", response["hits"]["total"]["value"])
print("Top search result:", response["hits"]["hits"][0]["_source"]["text"])

query = {
    "query": {
        "bool": {
            "must": [
                {"match": {"text": "chemistry"}},
                {"match": {"text": "daily life"}}
            ]
        }
    }
}

response = es.search(index="wiki", body=query)
print("Total hits:", response["hits"]["total"]["value"])
print("Top search result:", response["hits"]["hits"][0]["_source"]["text"])

query = {
    "query": {
        "match": {
            "text": "carbon atom"
        }
    },
    "highlight": {
        "fields": {
            "text": {}
        }
    }
}

response = es.search(index="wiki", body=query)
print("Total hits:", response["hits"]["total"]["value"])
print("Top search result with highlighting:")
print(response["hits"]["hits"][0]["highlight"]["text"][0])