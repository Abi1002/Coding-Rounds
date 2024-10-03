import csv
from elasticsearch import Elasticsearch

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

# Open and read the CSV file
with open('C:/Users/abigu/Downloads/employee/Employee Sample Data 1.csv', 'r') as file:

    reader = csv.DictReader(file)
    
    # Index each row in the CSV
    for row in reader:
        es.index(index='employee_data', body=row)

print("Data indexed successfully!")
