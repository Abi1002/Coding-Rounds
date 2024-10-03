from elasticsearch import Elasticsearch
import csv


es = Elasticsearch([{'scheme': 'http', 'host': 'localhost', 'port': 9200}])

def createCollection(p_collection_name):
    if not es.indices.exists(index=p_collection_name):
        es.indices.create(index=p_collection_name)
        print(f"Collection {p_collection_name} created.")
    else:
        print(f"Index {p_collection_name} already exists.")






def indexData(p_collection_name, p_exclude_column):
    csv_file_path = "C:\\Users\\abigu\\Downloads\\employee\\Employee Sample Data 1.csv"
    with open(csv_file_path, 'r', encoding='ISO-8859-1') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if p_exclude_column in row:
                del row[p_exclude_column]
            es.index(index=p_collection_name, body=row)
    print(f"Data indexed into {p_collection_name}, excluding {p_exclude_column}.")


    

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    body = {
        "query": {
            "match": {p_column_name: p_column_value}
        }
    }
    res = es.search(index=p_collection_name, body=body)
    print(f"Results for {p_column_name} = {p_column_value}:")
    for doc in res['hits']['hits']:
        print(doc['_source'])

def getEmpCount(p_collection_name):
    res = es.count(index=p_collection_name)
    print(f"Total employee count in {p_collection_name}: {res['count']}")

def delEmpById(p_collection_name, p_employee_id):
    try:
        
        res = es.get(index=p_collection_name, id=p_employee_id)  
        if res['found']:
            es.delete(index=p_collection_name, id=p_employee_id, ignore=[400, 404])
            print(f"Employee with ID {p_employee_id} deleted from {p_collection_name}.")
        else:
            print(f"Employee with ID {p_employee_id} not found in {p_collection_name}.")
    except Exception as e:
        print(f"Error deleting employee: {e}")




def getDepFacet(p_collection_name):
    body = {
        "size": 0,
        "aggs": {
            "department_count": {
                "terms": {
                    "field": "Department.keyword",
                    "size": 10
                }
            }
        }
    }
    res = es.search(index=p_collection_name, body=body)
    print("Employee count grouped by department:")
    for bucket in res['aggregations']['department_count']['buckets']:
        print(f"{bucket['key']}: {bucket['doc_count']}")


def searchById(p_collection_name, p_employee_id):
    try:
        res = es.get(index=p_collection_name, id=p_employee_id)
        if res['found']:
            print(f"Employee found: {res['_source']}")
        else:
            print(f"Employee with ID {p_employee_id} not found in {p_collection_name}.")
    except Exception as e:
        print(f"Error searching for employee: {e}")


def listAllEmployees(p_collection_name):
    try:
        res = es.search(index=p_collection_name, body={"query": {"match_all": {}}})
        print(f"Total employees in {p_collection_name}: {res['hits']['total']['value']}")
        for doc in res['hits']['hits']:
            print(f"ID: {doc['_id']}, Source: {doc['_source']}")
    except Exception as e:
        print(f"Error fetching employees: {e}")









v_nameCollection = 'hash_abinaya'
v_phoneCollection = 'hash_8186'  


createCollection(v_nameCollection)
createCollection(v_phoneCollection)


getEmpCount(v_nameCollection)

indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')

delEmpById(v_nameCollection, 'E02003')

getEmpCount(v_nameCollection)


searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')

getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
