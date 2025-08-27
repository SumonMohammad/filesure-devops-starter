from flask import Flask, request, jsonify, render_template_string
from prometheus_client import Counter, Summary, generate_latest, CONTENT_TYPE_LATEST
from pymongo import MongoClient
import os
import time
import random
from datetime import datetime
from bson import ObjectId

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
REQUEST_TIME = Summary('request_duration_seconds', 'Time spent processing request')
JOBS_CREATED = Counter('jobs_created_total', 'Total number of jobs created', ['num_jobs'])
JOBS_CREATED_FAILURES = Counter('jobs_created_failures', 'Number of job creation failures')
MONGODB_OPERATIONS = Counter('mongodb_operations_total', 'Total MongoDB operations', ['operation'])
MONGODB_OPERATION_TIME = Summary('mongodb_operation_duration_seconds', 'Time spent on MongoDB operations', ['operation'])

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
    business_types = ["SYSTEMS", "TECHNOLOGIES", "SOLUTIONS", "SERVICES", "PRODUCTS", "INNOVATIONS", "DIGITAL", "SOFTWARE"]
    prefixes = ["PRESSURE SENSITIVE", "DIGITAL", "SMART", "GLOBAL", "ADVANCED", "PREMIUM", "ELITE", "CORE"]
    
    prefix = random.choice(prefixes)
    business = random.choice(business_types)
    company_type = random.choice(company_types)
    
    return f"{prefix} {business} {company_type}"

@app.route("/")
def index():
    """Simple frontend to create jobs"""
    html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Job Creator - FileSure DevOps</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                max-width: 800px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .container {
                background: white;
                padding: 30px;
                border-radius: 10px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            }
            h1 {
                color: #333;
                text-align: center;
                margin-bottom: 30px;
            }
            .button-group {
                display: flex;
                gap: 10px;
                justify-content: center;
                flex-wrap: wrap;
                margin-bottom: 30px;
            }
            button {
                padding: 12px 24px;
                font-size: 16px;
                border: none;
                border-radius: 5px;
                cursor: pointer;
                transition: background-color 0.3s;
            }
            .btn-primary {
                background-color: #007bff;
                color: white;
            }
            .btn-primary:hover {
                background-color: #0056b3;
            }
            .btn-success {
                background-color: #28a745;
                color: white;
            }
            .btn-success:hover {
                background-color: #1e7e34;
            }
            .btn-warning {
                background-color: #ffc107;
                color: #212529;
            }
            .btn-warning:hover {
                background-color: #e0a800;
            }
            .btn-danger {
                background-color: #dc3545;
                color: white;
            }
            .btn-danger:hover {
                background-color: #c82333;
            }
            #result {
                margin-top: 20px;
                padding: 15px;
                border-radius: 5px;
                display: none;
            }
            .success {
                background-color: #d4edda;
                border: 1px solid #c3e6cb;
                color: #155724;
            }
            .error {
                background-color: #f8d7da;
                border: 1px solid #f5c6cb;
                color: #721c24;
            }
            .loading {
                background-color: #d1ecf1;
                border: 1px solid #bee5eb;
                color: #0c5460;
            }
            .job-list {
                margin-top: 20px;
                max-height: 300px;
                overflow-y: auto;
                border: 1px solid #ddd;
                border-radius: 5px;
                padding: 10px;
            }
            .job-item {
                padding: 8px;
                border-bottom: 1px solid #eee;
                font-size: 14px;
            }
            .job-item:last-child {
                border-bottom: none;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🚀 FileSure Job Creator</h1>
            
            <div class="button-group">
                <button class="btn-primary" onclick="createJobs(1)">Create 1 Job</button>
                <button class="btn-success" onclick="createJobs(3)">Create 3 Jobs</button>
                <button class="btn-warning" onclick="createJobs(5)">Create 5 Jobs</button>
                <button class="btn-danger" onclick="createJobs(10)">Create 10 Jobs</button>
            </div>
            
            <div style="text-align: center; margin-bottom: 20px;">
                <button class="btn-secondary" onclick="clearJobs()" style="background-color: #6c757d; color: white;">Clear Jobs List</button>
            </div>
            
            <div id="result"></div>
            
            <div class="job-list" id="jobList">
                <div style="text-align: center; color: #666;">No jobs created yet</div>
            </div>
        </div>

        <script>
            async function createJobs(numJobs) {
                const resultDiv = document.getElementById('result');
                const jobList = document.getElementById('jobList');
                
                // Show loading
                resultDiv.className = 'loading';
                resultDiv.style.display = 'block';
                resultDiv.innerHTML = `Creating ${numJobs} job(s)...`;
                
                try {
                    const response = await fetch('/create-job', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({ num_jobs: numJobs })
                    });
                    
                    const data = await response.json();
                    
                    if (response.ok) {
                        resultDiv.className = 'success';
                        resultDiv.innerHTML = `
                            <strong>✅ Success!</strong><br>
                            Created ${data.totalJobs} job(s)<br>
                            <small>Check MongoDB Compass to see the jobs</small>
                        `;
                        
                        // Update job list
                        updateJobList(data.jobs);
                    } else {
                        resultDiv.className = 'error';
                        resultDiv.innerHTML = `<strong>❌ Error:</strong> ${data.error}`;
                    }
                } catch (error) {
                    resultDiv.className = 'error';
                    resultDiv.innerHTML = `<strong>❌ Error:</strong> ${error.message}`;
                }
            }
            
            function updateJobList(jobs) {
                const jobList = document.getElementById('jobList');
                if (jobs.length === 0) {
                    jobList.innerHTML = '<div style="text-align: center; color: #666;">No jobs created yet</div>';
                    return;
                }
                
                let html = '<h3>Recently Created Jobs:</h3>';
                jobs.forEach((job, index) => {
                    html += `
                        <div class="job-item">
                            <strong>Job ${index + 1}:</strong> ${job.companyName}<br>
                            <small>CIN: ${job.cin} | ID: ${job.jobId}</small>
                        </div>
                    `;
                });
                jobList.innerHTML = html;
            }
            
            function clearJobs() {
                const jobList = document.getElementById('jobList');
                const resultDiv = document.getElementById('result');
                
                jobList.innerHTML = '<div style="text-align: center; color: #666;">No jobs created yet</div>';
                resultDiv.style.display = 'none';
            }
        </script>
    </body>
    </html>
    """
    return html

@app.route("/create-job", methods=["POST"])
@REQUEST_TIME.time()
def submit():
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
        
        MONGODB_OPERATIONS.labels(operation='insert_one').inc()
        MONGODB_OPERATION_TIME.labels(operation='insert_one').observe(mongo_time)
        
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
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)
