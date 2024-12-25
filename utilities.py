from elasticsearch import Elasticsearch
import uuid

#with Elastic Cloud details
CLOUD_ID = "9f9639dadbeb46e291e897fa2f7e0cba:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDZhODBmYTYzOWNkYTRjMDA5MmM4NjdmMzg0ODM1NmVhJDRjMmFiMzE0ZWI1YTQzMjJhYmYxMDQ4MzM4YWU3MjA3"
USERNAME = "elastic"
PASSWORD = "P9dnNRgD2uXmtRrbNtNzFSV0"

# Initialize Elasticsearch
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(USERNAME, PASSWORD))
index_name = "students"

def initialize_elasticsearch():
    return Elasticsearch(cloud_id=CLOUD_ID,basic_auth=(USERNAME, PASSWORD))

# Create a new student document
def create_student_doc(data):
    return {
        "id": str(uuid.uuid4()),
        "first_name": data.get("first_name"),
        "last_name": data.get("last_name"),
        "class": data.get("class"),
        "address": data.get("address"),
        "aggregate_score": data.get("aggregate_score"),
        "exam_scores": data.get("exam_scores", []),
    }

# Search for a student by ID
def search_student_by_id(es, index_name, student_id):
    query = {"query": {"match": {"id": student_id}}}
    res = es.search(index=index_name, body=query)
    if res["hits"]["hits"]:
        return res["hits"]["hits"][0]["_source"]
    return None

# Update a student document
def update_student_doc(es, index_name, student_id, data):
    student = search_student_by_id(es, index_name, student_id)
    if not student:
        return {"error": "Student not found"}

    updated_fields = {}
    immutable_fields = ["first_name", "last_name", "class"]
    ignored_fields = []

    for key, value in data.items():
        if key in immutable_fields:
            ignored_fields.append(key)
        else:
            student[key] = value
            updated_fields[key] = value

    # Update in Elasticsearch
    query = {"query": {"match": {"id": student_id}}}
    res = es.search(index=index_name, body=query)
    doc_id = res["hits"]["hits"][0]["_id"]
    es.update(index=index_name, id=doc_id, body={"doc": student})

    return {
        "message": "Student updated successfully",
        "ignored_fields": ignored_fields,
        "updated_fields": updated_fields
    }

# Delete a student document
def delete_student_doc(es, index_name, student_id):
    query = {"query": {"match": {"id": student_id}}}
    res = es.search(index=index_name, body=query)
    if not res["hits"]["hits"]:
        return {"error": "Student not found"}
    doc_id = res["hits"]["hits"][0]["_id"]
    es.delete(index=index_name, id=doc_id)
    return {"message": "Student deleted successfully"}

# Sort students by a specific exam type
def sort_students_by_exam_type(es, index_name, exam_type):
    query = {
        "query": {"nested": {
            "path": "exam_scores",
            "query": {"match": {"exam_scores.type": exam_type}}
        }},
        "sort": [
            {"exam_scores.percentage": {"order": "desc", "nested_path": "exam_scores"}}
        ]
    }
    res = es.search(index=index_name, body=query)
    return [hit["_source"] for hit in res["hits"]["hits"]]
