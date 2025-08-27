from flask import Flask, request, jsonify
from prometheus_client import Counter, Summary, generate_latest, CONTENT_TYPE_LATEST
from pymongo import MongoClient
import os
import time
import random
from datetime import datetime

app = Flask(__name__)

# Mongo setup
mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(mongo_uri)
db = client["filesure"]
collection = db["jobs"]
docs_collection = db["documents"]

# Ensure database and collections exist
try:
    # This will create the collections if they don't exist
    db.create_collection("jobs")
    db.create_collection("documents")

    # Create index for lock expiration queries
    try:
        collection.create_index("lockedAt")
        print("Index created for lock expiration queries")
    except Exception as e:
        print(f"Index setup: {e}")

    print("Database and collections setup completed")
except Exception as e:
    # Collections might already exist, which is fine
    print(f"Collections setup: {e}")

# Prometheus metrics
REQUEST_COUNT = Counter('requests_total', 'Total number of requests')
REQUEST_FAILS = Counter('db_write_failures', 'Number of DB write failures')
REQUEST_TIME = Summary(
    'request_duration_seconds',
    'Time spent processing request'
)
JOBS_CREATED = Counter('jobs_created_total', 'Total number of jobs created', ['num_jobs'])
JOBS_CREATED_FAILURES = Counter('jobs_created_failures', 'Number of job creation failures')
MONGODB_OPERATIONS = Counter('mongodb_operations_total', 'Total MongoDB operations', ['operation'])
MONGODB_OPERATION_TIME = Summary(
    'mongodb_operation_duration_seconds',
    'Time spent on MongoDB operations',
    ['operation']
)


def generate_random_cin():
    """Generate a random CIN number"""
    # CIN format: L24295GJ1987PLC143792 (L + 5 digits + 2 letters + 4 digits + 3 letters + 6 digits)
    digits = ''.join([str(random.randint(0, 9)) for _ in range(5)])
    letters1 = ''.join([chr(random.randint(65, 90)) for _ in range(2)])
    year = str(random.randint(1980, 2020))
    letters2 = ''.join([chr(random.randint(65, 90)) for _ in range(3)])
    final_digits = ''.join([str(random.randint(0, 9)) for _ in range(6)])
    return f"L{digits}{letters1}{year}{letters2}{final_digits}"


def generate_random_company_name():
    """Generate a random company name"""
    company_types = ["LIMITED", "PRIVATE LIMITED", "LLP"]
    business_types = [
        "SYSTEMS", "TECHNOLOGIES", "SOLUTIONS", "SERVICES", "PRODUCTS",
        "INNOVATIONS", "DIGITAL", "SOFTWARE"
    ]
    prefixes = [
        "PRESSURE SENSITIVE", "DIGITAL", "SMART", "GLOBAL", "ADVANCED",
        "PREMIUM", "ELITE", "CORE"
    ]

    prefix = random.choice(prefixes)
    business = random.choice(business_types)
    company_type = random.choice(company_types)

    return f"{prefix} {business} {company_type}"


@app.route("/")
def index():
    """Simple frontend to create jobs"""
    html = (
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "    <meta charset=\"UTF-8\">\n"
        "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n"
        "    <title>Job Creator - FileSure DevOps</title>\n"
        "    <style>\n"
        "        body {\n"
        "            font-family: Arial, sans-serif;\n"
        "            max-width: 800px;\n"
        "            margin: 0 auto;\n"
        "            padding: 20px;\n"
        "            background-color: #f5f5f5;\n"
        "        }\n"
        "        .container {\n"
        "            background: white;\n"
        "            padding: 30px;\n"
        "            border-radius: 10px;\n"
        "            box-shadow: 0 2px 10px rgba(0,0,0,0.1);\n"
        "        }\n"
        "        h1 {\n"
        "            color: #333;\n"
        "            text-align: center;\n"
        "            margin-bottom: 30px;\n"
        "        }\n"
        "        .button-group {\n"
        "            display: flex;\n"
        "            gap: 10px;\n"
        "            justify-content: center;\n"
        "            flex-wrap: wrap;\n"
        "            margin-bottom: 30px;\n"
        "        }\n"
        "        button {\n"
        "            padding: 12px 24px;\n"
        "            font-size: 16px;\n"
        "            border: none;\n"
        "            border-radius: 5px;\n"
        "            cursor: pointer;\n"
        "            transition: background-color 0.3s;\n"
        "        }\n"
        "        .btn-primary {\n"
        "            background-color: #007bff;\n"
        "            color: white;\n"
        "        }\n"
        "        .btn-primary:hover {\n"
        "            background-color: #0056b3;\n"
        "        }\n"
        "        .btn-success {\n"
        "            background-color: #28a745;\n"
        "            color: white;\n"
        "        }\n"
        "        .btn-success:hover {\n"
        "            background-color: #1e7e34;\n"
        "        }\n"
        "        .btn-warning {\n"
        "            background-color: #ffc107;\n"
        "            color: #212529;\n"
        "        }\n"
        "        .btn-warning:hover {\n"
        "            background-color: #e0a800;\n"
        "        }\n"
        "        .btn-danger {\n"
        "            background-color: #dc3545;\n"
        "            color: white;\n"
        "        }\n"
        "        .btn-danger:hover {\n"
        "            background-color: #c82333;\n"
        "        }\n"
        "        #result {\n"
        "            margin-top: 20px;\n"
        "            padding: 15px;\n"
        "            border-radius: 5px;\n"
        "            display: none;\n"
        "        }\n"
        "        .success {\n"
        "            background-color: #d4edda;\n"
        "            border: 1px solid #c3e6cb;\n"
        "            color: #155724;\n"
        "        }\n"
        "        .error {\n"
        "            background-color: #f8d7da;\n"
        "            border: 1px solid #f5c6cb;\n"
        "            color: #721c24;\n"
        "        }\n"
        "        .loading {\n"
        "            background-color: #d1ecf1;\n"
        "            border: 1px solid #bee5eb;\n"
        "            color: #0c5460;\n"
        "        }\n"
        "        .job-list {\n"
        "            margin-top: 20px;\n"
        "            max-height: 300px;\n"
        "            overflow-y: auto;\n"
        "            border: 1px solid #ddd;\n"
        "            border-radius: 5px;\n"
        "            padding: 10px;\n"
        "        }\n"
        "        .job-item {\n"
        "            padding: 8px;\n"
        "            border-bottom: 1px solid #eee;\n"
        "            font-size: 14px;\n"
        "        }\n"
        "        .job-item:last-child {\n"
        "            border-bottom: none;\n"
        "        }\n"
        "    </style>\n"
        "</head>\n"
        "<body>\n"
        "    <div class=\"container\">\n"
        "        <h1>🚀 FileSure Job Creator</h1>\n"
        "        <div class=\"button-group\">\n"
        "            <button class=\"btn-primary\" onclick=\"createJobs(1)\">Create 1 Job</button>\n"
        "            <button class=\"btn-success\" onclick=\"createJobs(3)\">Create 3 Jobs</button>\n"
        "            <button class=\"btn-warning\" onclick=\"createJobs(5)\">Create 5 Jobs</button>\n"
        "            <button class=\"btn-danger\" onclick=\"createJobs(10)\">Create 10 Jobs</button>\n"
        "        </div>\n"
        "        <div style=\"text-align: center; margin-bottom: 20px;\">\n"
        "            <button class=\"btn-secondary\" onclick=\"clearJobs()\" "
        "style=\"background-color: #6c757d; color: white;\">Clear Jobs List</button>\n"
        "        </div>\n"
        "        <div id=\"result\"></div>\n"
        "        <div class=\"job-list\" id=\"jobList\">\n"
        "            <div style=\"text-align: center; color: #666;\">No jobs created yet</div>\n"
        "        </div>\n"
        "    </div>\n"
        "    <script>\n"
        "        async function createJobs(numJobs) {\n"
        "            const resultDiv = document.getElementById('result');\n"
        "            const jobList = document.getElementById('jobList');\n"
        "            resultDiv.className = 'loading';\n"
        "            resultDiv.style.display = 'block';\n"
        "            resultDiv.innerHTML = `Creating ${numJobs} job(s)...`;\n"
        "            try {\n"
        "                const response = await fetch('/create-job', {\n"
        "                    method: 'POST',\n"
        "                    headers: {\n"
        "                        'Content-Type': 'application/json',\n"
        "                    },\n"
        "                    body: JSON.stringify({ num_jobs: numJobs })\n"
        "                });\n"
        "                const data = await response.json();\n"
        "                if (response.ok) {\n"
        "                    resultDiv.className = 'success';\n"
        "                    resultDiv.innerHTML = `\n"
        "                        <strong>✅ Success!</strong><br>\n"
        "                        Created ${data.totalJobs} job(s)<br>\n"
        "                        <small>Check MongoDB Compass to see the jobs</small>\n"
        "                    `;\n"
        "                    updateJobList(data.jobs);\n"
        "                } else {\n"
        "                    resultDiv.className = 'error';\n"
        "                    resultDiv.innerHTML = `<strong>❌ Error:</strong> ${data.error}`;\n"
        "                }\n"
        "            } catch (error) {\n"
        "                resultDiv.className = 'error';\n"
        "                resultDiv.innerHTML = `<strong>❌ Error:</strong> ${error.message}`;\n"
        "            }\n"
        "        }\n"
        "        function updateJobList(jobs) {\n"
        "            const jobList = document.getElementById('jobList');\n"
        "            if (jobs.length === 0) {\n"
        "                jobList.innerHTML = '<div style=\"text-align: center; color: #666;\">"
        "No jobs created yet</div>';\n"
        "                return;\n"
        "            }\n"
        "            let html = '<h3>Recently Created Jobs:</h3>';\n"
        "            jobs.forEach((job, index) => {\n"
        "                html += `\n"
        "                    <div class=\"job-item\">\n"
        "                        <strong>Job ${index + 1}:</strong> ${job.companyName}<br>\n"
        "                        <small>CIN: ${job.cin} | ID: ${job.jobId}</small>\n"
        "                    </div>\n"
        "                `;\n"
        "            });\n"
        "            jobList.innerHTML = html;\n"
        "        }\n"
        "        function clearJobs() {\n"
        "            const jobList = document.getElementById('jobList');\n"
        "            const resultDiv = document.getElementById('result');\n"
        "            jobList.innerHTML = '<div style=\"text-align: center; color: #666;\">"
        "No jobs created yet</div>';\n"
        "            resultDiv.style.display = 'none';\n"
        "        }\n"
        "    </script>\n"
        "</body>\n"
        "</html>\n"
    )
    return html


@app.route("/create-job", methods=["POST"])
@REQUEST_TIME.time()
def submit():
    """Handle job creation request"""
    REQUEST_COUNT.inc()
    data = request.json
    num_jobs = data.get("num_jobs", 1)  # Default to 1 if not specified

    # Validate num_jobs range
    try:
        num_jobs = int(num_jobs)
        if num_jobs < 1 or num_jobs > 10:
            return jsonify({"error": "num_jobs must be between 1 and 10"}), 400
    except (ValueError, TypeError):
        return jsonify({"error": "num_jobs must be a valid integer"}), 400

    try:
        current_time = datetime.utcnow()
        created_jobs = []

        # Create multiple job documents
        for i in range(num_jobs):
            job_doc = {
                "cin": generate_random_cin(),
                "companyName": generate_random_company_name(),
                "jobStatus": "pending",
                "processingStages": {
                    "documentDownload": {
                        "status": "pending",
                        "totalDocuments": 0,
                        "downloadedDocuments": 0,
                        "pendingDocuments": 0,
                        "lastUpdated": current_time
                    }
                },
                "createdAt": current_time,
                "updatedAt": current_time
            }

            # Track MongoDB operation time
            start_time = time.time()
            result = collection.insert_one(job_doc)
            mongo_time = time.time() - start_time

            MONGODB_OPERATIONS.labels(operation="insert_one").inc()
            MONGODB_OPERATION_TIME.labels(operation="insert_one").observe(mongo_time)

            created_jobs.append({
                "jobId": str(result.inserted_id),
                "cin": job_doc["cin"],
                "companyName": job_doc["companyName"]
            })

        # Track successful job creation
        JOBS_CREATED.labels(num_jobs=str(num_jobs)).inc()

        return jsonify({
            "message": f"Successfully created {num_jobs} job(s)",
            "jobs": created_jobs,
            "totalJobs": num_jobs
        }), 200

    except Exception as e:
        REQUEST_FAILS.inc()
        JOBS_CREATED_FAILURES.inc()
        return jsonify({"error": str(e)}), 500


@app.route("/metrics")
def metrics():
    """Expose Prometheus metrics"""
    return generate_latest(), 200, {"Content-Type": CONTENT_TYPE_LATEST}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)