import os
from sys import stderr

from flask import Flask, jsonify, request
from flask_cors import CORS

from run import handler as pipeline_run

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

api_key = os.environ.get("API_KEY", "")
if api_key == "":
    print("api key is required", file=stderr)

api_base_url = "https://api.stagingv3.microgen.id/query/api/v1/" + api_key

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask</h2>'

@app.route('/pipeline-run', methods=['POST'])
def pipeline_runs():
    return pipeline_run(request, jsonify)


if __name__ == "__main__":
    app.run(debug=True)