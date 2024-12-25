from flask import Flask
from route import student_routes

app = Flask(__name__)

# Register blueprints
app.register_blueprint(student_routes, url_prefix="/students")

if __name__ == "__main__":
    app.run(debug=True)
