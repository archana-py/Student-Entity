from flask import Flask, request, jsonify
from elasticsearch import Elasticsearch
import uuid

#with Elastic Cloud details
CLOUD_ID = "9f9639dadbeb46e291e897fa2f7e0cba:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJDZhODBmYTYzOWNkYTRjMDA5MmM4NjdmMzg0ODM1NmVhJDRjMmFiMzE0ZWI1YTQzMjJhYmYxMDQ4MzM4YWU3MjA3"
USERNAME = "elastic"
PASSWORD = "P9dnNRgD2uXmtRrbNtNzFSV0"

app = Flask(__name__)

# Initialize Elasticsearch
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=(USERNAME, PASSWORD))
index_name = "students"

# Ensure the index exists
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)

# Student schema
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


@app.route("/students", methods=["POST"])
def create_student():
    data = request.json
    doc = create_student_doc(data)
    res = es.index(index=index_name, document=doc)
    return jsonify({"message": "Student created successfully", "student_id": doc["id"]}), 201


@app.route("/students/<student_id>", methods=["GET"])
def get_student(student_id):
    query = {"query": {"match": {"id": student_id}}}
    res = es.search(index=index_name, body=query)
    if res["hits"]["hits"]:
        return jsonify(res["hits"]["hits"][0]["_source"]), 200
    return jsonify({"error": "Student not found"}), 404


@app.route("/students/<student_id>", methods=["PUT"])
def update_student(student_id):
    data = request.json
    query = {"query": {"match": {"id": student_id}}}
    res = es.search(index=index_name, body=query)

    if not res["hits"]["hits"]:
        return jsonify({"error": "Student not found"}), 404

    student = res["hits"]["hits"][0]["_source"]
    updated_fields = {}


    immutable_fields = ["first_name", "last_name", "class"]
    ignored_fields = []

    for key, value in data.items():
        if key in immutable_fields:
            ignored_fields.append(key)
        else:
            student[key] = value
            updated_fields[key] = value


    doc_id = res["hits"]["hits"][0]["_id"]
    es.update(index=index_name, id=doc_id, body={"doc": student})

    return jsonify({
        "message": "Student updated successfully",
        "ignored_fields": ignored_fields,
        "updated_fields": updated_fields
    }), 200


@app.route("/students/<student_id>", methods=["DELETE"])
def delete_student(student_id):
    query = {"query": {"match": {"id": student_id}}}
    res = es.search(index=index_name, body=query)

    if not res["hits"]["hits"]:
        return jsonify({"error": "Student not found"}), 404

    doc_id = res["hits"]["hits"][0]["_id"]
    es.delete(index=index_name, id=doc_id)
    return jsonify({"message": "Student deleted successfully"}), 200

@app.route("/students/exam-scores", methods=["GET"])
def get_students_by_exam_score():
    exam_type = request.args.get("exam_type")

    if not exam_type:
        return jsonify({"error": "exam_type is required"}), 400

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
    students = [hit["_source"] for hit in res["hits"]["hits"]]

    return jsonify(students), 200

if __name__ == "__main__":
    app.run(debug=True)
