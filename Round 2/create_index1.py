from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

index_mapping = {
    "mappings": {
        "properties": {
            "name": {"type": "text"},
            "age": {"type": "integer"},
            "department": {"type": "text"},
            "salary": {"type": "integer"},
        }
    }
}

es.indices.create(index='employee_data', body=index_mapping)
print("Index created successfully!")
