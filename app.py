from flask import Flask, request, jsonify
from flask_cors import CORS
from google.cloud import bigquery, storage

# flask
app = Flask(__name__)
CORS(app)

# sesuaikan nama dataset sql
bq_client = bigquery.Client()
DATASET = "library_data"
BOOKS_TABLE = f"{DATASET}.books"

# Buat dan seuaikan sama bucket
storage_client = storage.Client()
BUCKET_NAME = "your-bucket-name"

# routes
@app.route('/')
def home():
    return jsonify({"message": "Welcome to Book Borrowing Forum API!"})

# Upload Book to BigQuery
@app.route('/books/upload', methods=['POST'])
def upload_book():
    data = request.json
    try:
        rows_to_insert = [
            {
                "book_id": data["book_id"],
                "title": data["title"],
                "description": data["description"],
                "owner_id": data["owner_id"],
                "availability": True
            }
        ]
        errors = bq_client.insert_rows_json(BOOKS_TABLE, rows_to_insert)
        if errors:
            return jsonify({"error": errors}), 400
        return jsonify({"message": "Book uploaded successfully"}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Get All Available Books from BigQuery
@app.route('/books', methods=['GET'])
def get_books():
    try:
        query = f"SELECT * FROM `{BOOKS_TABLE}` WHERE availability = TRUE"
        query_job = bq_client.query(query)
        books = [dict(row) for row in query_job]
        return jsonify(books), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Upload File to Cloud Storage
@app.route('/upload-image', methods=['POST'])
def upload_image():
    try:
        file = request.files['file']
        blob = storage_client.bucket(BUCKET_NAME).blob(file.filename)
        blob.upload_from_file(file)
        return jsonify({"message": "Image uploaded", "url": blob.public_url}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Load Dataset from Cloud Storage
@app.route('/load-dataset', methods=['GET'])
def load_dataset():
    try:
        blob = storage_client.bucket(BUCKET_NAME).blob("merged_books_ratings.csv")
        dataset_content = blob.download_as_text()
        return jsonify({"message": "Dataset loaded", "content": dataset_content}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Run App
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
