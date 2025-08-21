# FileSure DevOps Simulation

This project simulates a **document download processing system** with job queuing, processing, and monitoring capabilities. It demonstrates a real-world scenario where documents need to be downloaded in batches with progress tracking.

---

## 🏗️ System Architecture

### **API Service (`api/app.py`)**

- **Web Interface**: Beautiful UI at `http://localhost:5001` for creating test jobs
- **Job Creation**: REST API endpoint to create 1-10 jobs with random company data
- **MongoDB Integration**: Stores jobs with CIN numbers, company names, and processing status
- **Prometheus Metrics**: `/metrics` endpoint for monitoring request counts and failures

### **Downloader Worker (`worker/downloader.py`)**

- **Job Processing**: Processes a single job by ID and exits (KEDA-friendly)
- **Request-Based**: Accepts job ID via command line or environment variable
- **Scaling Ready**: Perfect for KEDA-based scaling (one pod per job)
- **Fault Tolerant**: Job locking prevents conflicts, automatic cleanup of expired locks
- **Azure Blob Storage**: Uploads downloaded documents as text files to Azure Blob Storage
- **Document Tracking**: Saves document metadata to MongoDB documents collection
- **Realistic Simulation**: Downloads 35% of documents per run, with 30-50 total documents per job

---

## 🎯 Key Features

### **Job Management**

- **Random Data Generation**: Creates realistic CIN numbers and company names
- **Status Tracking**: Jobs progress through `pending` → `processing` → `in_progress` → `completed`
- **Progress Monitoring**: Real-time updates on document download progress
- **Document Processing**: Generates realistic text files with company information
- **Dual Storage**: Documents stored in Azure Blob Storage + metadata in MongoDB

### **Scalability & Reliability**

- **Job Locking**: Prevents multiple workers from processing the same job
- **Auto-Recovery**: Expired locks are automatically cleaned up
- **Horizontal Scaling**: Multiple worker instances can run simultaneously
- **Fault Tolerance**: Failed containers don't block the entire system

### **Monitoring & Observability**

- **Comprehensive Metrics**: 15+ Prometheus metrics across API and worker
- **Multi-Service Monitoring**: Separate metrics endpoints (API:5001, Worker:9100)
- **Blob Storage Metrics**: Upload counts, operation times, success rates
- **Job Processing Metrics**: Processing times, batch sizes, lock management
- **Structured Logging**: Detailed logs for debugging and monitoring
- **Real-time Progress**: Track job status and download progress

---

## 📊 Job Processing Flow

### **Job Creation**

1. User creates jobs via web interface or API
2. Jobs stored in MongoDB `jobs` collection with `pending` status
3. Each job gets random CIN and company name
4. System creates `documents` collection for tracking downloaded files

### **Job Processing**

1. KEDA detects pending job and triggers worker pod
2. Worker acquires lock on specific job ID
3. First run: Sets random total documents (30-50)
4. Downloads 35% of documents per run
5. **For each document:**
   - Generates realistic text file content
   - Uploads to Azure Blob Storage as `.txt` file
   - Saves document metadata to MongoDB documents collection
   - Updates progress in real-time
6. Releases lock when done or sets back to pending
7. Worker pod exits (success/failure)

### **Completion**

- Jobs complete after 2-3 runs (depending on total documents)
- Final status: `completed` with all documents downloaded
- **Documents stored**: Text files in Azure Blob Storage
- **Metadata tracked**: Document info in MongoDB `documents` collection
- Lock fields automatically cleaned up

---

## 🛠️ Technology Stack

- **Backend**: Python Flask
- **Database**: MongoDB
- **Cloud Storage**: Azure Blob Storage
- **Monitoring**: Prometheus metrics
- **Job Processing**: Custom worker with locking mechanism
- **Frontend**: Simple HTML/CSS/JavaScript interface

---

## 🚀 Getting Started

### **Prerequisites**

- Python 3.9+
- MongoDB (local or remote)
- Required packages (see `requirements.txt`)

### **Environment Variables**

- `MONGO_URI`: MongoDB connection string (defaults to `mongodb://localhost:27017`)
- `AZURE_BLOB_CONN`: Azure Blob Storage connection string
- `AZURE_CONTAINER`: Azure Blob Storage container name (defaults to `documents`)
- `JOB_ID`: Job ID for worker processing (set by KEDA or manually for testing)

See `env.example` for all available environment variables.

### **Running the System Locally**

1. Start MongoDB
2. Run API: `cd api && python app.py`
3. Create jobs via web interface: `http://localhost:5001`
4. Run Worker for specific job: `cd worker && python downloader.py <job_id>`
   - Example: `python downloader.py 68a6b73df6d117b0aeaa695e`
   - Or: `JOB_ID=68a6b73df6d117b0aeaa695e python downloader.py`

### **Containerization**

This project provides the application code only. You will need to:

- Create Dockerfiles for both API and worker services
- Set up docker-compose.yml for local development
- Implement Kubernetes manifests for production deployment
- Configure CI/CD pipelines for automated deployment

---

## 📈 Monitoring

### **Prometheus Metrics**

**API Metrics (`/metrics` on port 5001):**

- `requests_total`: Total API requests
- `db_write_failures`: Database write failures
- `request_duration_seconds`: Request processing time
- `jobs_created_total{num_jobs="X"}`: Jobs created by batch size
- `jobs_created_failures`: Job creation failures
- `mongodb_operations_total{operation="insert_one"}`: MongoDB operations
- `mongodb_operation_duration_seconds{operation="insert_one"}`: MongoDB operation time

**Worker Metrics (`/metrics` on port 9100):**

- `jobs_processed_total{status="completed|pending"}`: Jobs processed by status
- `jobs_found_total`: Jobs found in each cycle
- `jobs_locked_total`: Successfully locked jobs
- `jobs_failed_total`: Failed job processing
- `documents_downloaded_total`: Total documents downloaded
- `documents_uploaded_total`: Total documents uploaded to blob storage
- `blob_operations_total{operation="upload"}`: Blob storage operations
- `blob_operation_duration_seconds{operation="upload"}`: Blob operation times
- `job_processing_duration_seconds`: Job processing time
- `lock_cleanup_total`: Expired locks cleaned up
- `active_jobs`: Currently processing jobs (Gauge)
- `pending_jobs`: Jobs waiting to be processed (Gauge)
- `completed_jobs`: Completed jobs (Gauge)
- `download_batch_size`: Documents per batch (Histogram)

### **MongoDB Queries**

**Jobs Collection:**

- Find pending jobs: `{"jobStatus": "pending"}`
- Find processing jobs: `{"jobStatus": "processing"}`
- Find completed jobs: `{"jobStatus": "completed"}`

**Documents Collection:**

- Find documents by job: `{"jobId": ObjectId("...")}`
- Find documents by company: `{"companyName": "COMPANY NAME"}`
- Find documents by type: `{"documentType": "Annual Report"}`

---

## 🔧 System Design Decisions

### **Job Locking Strategy**

- Uses MongoDB's `findOneAndUpdate` for atomic operations
- Prevents race conditions between multiple workers
- Automatic lock expiration prevents stuck jobs

### **Document Processing Strategy**

- **Text Files**: Realistic document content instead of JSON
- **Dual Storage**: Files in Azure Blob Storage, metadata in MongoDB
- **Rich Metadata**: Document type, size, checksum, blob URL tracking
- **Organized Structure**: `jobs/{job_id}/document_{number}.txt` blob paths

### **Progress Simulation**

- 35% progress per run ensures realistic batch processing
- 1-second delay per document simulates real download time
- Random total documents (30-50) adds variability

### **Fault Tolerance**

- Lock cleanup prevents system deadlocks
- Graceful handling of container failures
- Automatic job recovery without manual intervention
- Azure Blob Storage fallback (works without Azure configured)

---

This simulation provides a realistic foundation for testing containerization, orchestration, and monitoring solutions in a production-like environment.

---

## 📋 Project Structure

```
filesure-devops-starter/
├── api/
│   └── app.py              # Flask API with web interface
├── worker/
│   └── downloader.py       # Job processing worker
├── requirements.txt        # Python dependencies
├── env.example            # Environment variables template
└── README.md              # This documentation
```

The project contains all the application logic needed to simulate a document processing system. The containerization, orchestration, and deployment infrastructure will be implemented by candidates as part of their assignment.
