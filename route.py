from flask import Blueprint, request, jsonify
from utilities import (
    initialize_elasticsearch,
    create_student_doc,
    search_student_by_id,
    update_student_doc,
    delete_student_doc,
    sort_students_by_exam_type
)

# Create blueprint
student_routes = Blueprint("student_routes", __name__)
es = initialize_elasticsearch()
index_name = "students"

# Ensure the index exists
if not es.indices.exists(index=index_name):
    es.indices.create(index=index_name)

@student_routes.route("/", methods = ["GET"])
def home():
    return "Welcome to the Student API"

# API to create a new student
@student_routes.route("/", methods=["POST"])
def create_student():
    data = request.json
    doc = create_student_doc(data)
    es.index(index=index_name, document=doc)
    return jsonify({"message": "Student created successfully", "student_id": doc["id"]}), 201

# API to retrieve a student by ID
@student_routes.route("/<student_id>", methods=["GET"])
def get_student(student_id):
    student = search_student_by_id(es, index_name, student_id)
    if not student:
        return jsonify({"error": "Student not found"}), 404
    return jsonify(student), 200

# API to update a student
@student_routes.route("/<student_id>", methods=["PUT"])
def update_student(student_id):
    data = request.json
    result = update_student_doc(es, index_name, student_id, data)
    if result.get("error"):
        return jsonify(result), 404
    return jsonify(result), 200

# API to delete a student
@student_routes.route("/<student_id>", methods=["DELETE"])
def delete_student(student_id):
    result = delete_student_doc(es, index_name, student_id)
    if result.get("error"):
        return jsonify(result), 404
    return jsonify(result), 200

# API to get students sorted by exam score for a specific exam type
@student_routes.route("/exam-scores", methods=["GET"])
def get_students_by_exam_score():
    exam_type = request.args.get("exam_type")
    if not exam_type:
        return jsonify({"error": "exam_type is required"}), 400
    students = sort_students_by_exam_type(es, index_name, exam_type)
    return jsonify(students), 200
