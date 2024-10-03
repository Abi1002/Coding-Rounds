from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])

# Search for all documents in the employee_data index
res = es.search(index="employee_data", body={"query": {"match_all": {}}})

# Print the results
for doc in res['hits']['hits']:
    print(doc['_source'])
