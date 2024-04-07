from elasticsearch import Elasticsearch

# Create an Elasticsearch client instance
es = Elasticsearch("http://localhost:9200")

# Retrieve all cluster settings
all_settings = es.cluster.get_settings(flat_settings=True, include_defaults=True)

# Filter the settings to get only the thread pool settings
thread_pool_settings = {
    k: v for k, v in all_settings.get("defaults", {}).items() if k.startswith("thread_pool.")
}

# Print the thread pool settings
print(thread_pool_settings)