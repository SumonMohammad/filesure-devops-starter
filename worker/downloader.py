import os
import time
import random
import sys
import json
import threading
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
import logging
from prometheus_client import Counter, Summary, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from azure.storage.blob import BlobServiceClient
from flask import Flask

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DocumentDownloader:
    def __init__(self):
        # MongoDB setup
        self.mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client["filesure"]
        self.collection = self.db["jobs"]
        self.docs_collection = self.db["documents"]
        
        # Azure Blob Storage setup
        self.blob_connection_string = os.environ.get("AZURE_BLOB_CONN")
        self.blob_container = os.environ.get("AZURE_CONTAINER", "documents")
        self.blob_client = None
        
        if self.blob_connection_string:
            try:
                self.blob_service_client = BlobServiceClient.from_connection_string(self.blob_connection_string)
                logger.info("Azure Blob Storage client initialized")
            except Exception as e:
                logger.warning(f"Azure Blob Storage not configured: {e}")
                self.blob_service_client = None
        else:
            logger.warning("Azure Blob Storage connection string not provided")
            self.blob_service_client = None
        
        # Prometheus metrics
        self.jobs_processed = Counter('jobs_processed_total', 'Total number of jobs processed', ['status'])
        self.jobs_found = Counter('jobs_found_total', 'Total number of jobs found in each cycle')
        self.jobs_locked = Counter('jobs_locked_total', 'Total number of jobs successfully locked')
        self.jobs_failed = Counter('jobs_failed_total', 'Total number of jobs that failed processing')
        self.documents_downloaded = Counter('documents_downloaded_total', 'Total number of documents downloaded')
        self.documents_uploaded = Counter('documents_uploaded_total', 'Total number of documents uploaded to blob storage')
        self.blob_operations = Counter('blob_operations_total', 'Total blob storage operations', ['operation'])
        self.blob_operation_time = Summary('blob_operation_duration_seconds', 'Time spent on blob operations', ['operation'])
        self.processing_time = Summary('job_processing_duration_seconds', 'Time spent processing jobs')
        self.lock_cleanup = Counter('lock_cleanup_total', 'Total number of expired locks cleaned up')
        self.active_jobs = Gauge('active_jobs', 'Number of jobs currently being processed')
        self.pending_jobs = Gauge('pending_jobs', 'Number of jobs waiting to be processed')
        self.completed_jobs = Gauge('completed_jobs', 'Number of jobs completed')
        self.download_batch_size = Histogram('download_batch_size', 'Number of documents downloaded per batch')
        
        # Ensure database and collections exist
        try:
            # This will create the collections if they don't exist
            self.db.create_collection("jobs")
            self.db.create_collection("documents")
            
            # Create index for lock expiration queries
            try:
                self.collection.create_index("lockedAt")
                logger.info("Index created for lock expiration queries")
            except Exception as e:
                logger.info(f"Index setup: {e}")
                
            logger.info("Database and collections setup completed")
        except Exception as e:
            # Collections might already exist, which is fine
            logger.info(f"Collections setup: {e}")
        
        logger.info("Document Downloader initialized")
    
    def get_job_by_id(self, job_id):
        """Get a specific job by ID and try to acquire lock"""
        try:
            # Convert string job_id to ObjectId
            try:
                object_id = ObjectId(job_id)
            except Exception as e:
                logger.error(f"Invalid job ID format: {job_id}")
                return None
            
            # Try to acquire lock on the specific job
            job = self.collection.find_one_and_update(
                {"_id": object_id, "jobStatus": "pending"},
                {
                    "$set": {
                        "jobStatus": "processing",
                        "processingStages.documentDownload.status": "processing",
                        "processingStages.documentDownload.lastUpdated": datetime.utcnow(),
                        "updatedAt": datetime.utcnow(),
                        "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
                        "lockedAt": datetime.utcnow()
                    }
                },
                return_document=True
            )
            
            if job:
                self.jobs_locked.inc()
                self.active_jobs.inc()
                logger.info(f"Acquired lock on job {job['_id']} for {job.get('companyName', 'Unknown')}")
                return job
            else:
                logger.warning(f"Job {job_id} not found or not in pending status")
                return None
                
        except Exception as e:
            logger.error(f"Error getting job {job_id}: {e}")
            return None
    
    def _cleanup_expired_locks(self):
        """Clean up locks that are older than 10 minutes"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(minutes=10)
            result = self.collection.update_many(
                {
                    "jobStatus": "processing",
                    "lockedAt": {"$lt": cutoff_time}
                },
                {
                    "$set": {
                        "jobStatus": "pending",
                        "processingStages.documentDownload.status": "pending",
                        "processingStages.documentDownload.lastUpdated": datetime.utcnow(),
                        "updatedAt": datetime.utcnow()
                    },
                    "$unset": {
                        "lockedBy": "",
                        "lockedAt": ""
                    }
                }
            )
            
            if result.modified_count > 0:
                self.lock_cleanup.inc(result.modified_count)
                logger.info(f"Cleaned up {result.modified_count} expired locks")
                
        except Exception as e:
            logger.error(f"Error cleaning up expired locks: {e}")
    
    def _update_job_counts(self):
        """Update job count metrics"""
        try:
            pending_count = self.collection.count_documents({"jobStatus": "pending"})
            active_count = self.collection.count_documents({"jobStatus": "processing"})
            completed_count = self.collection.count_documents({"jobStatus": "completed"})
            
            self.pending_jobs.set(pending_count)
            self.active_jobs.set(active_count)
            self.completed_jobs.set(completed_count)
            
        except Exception as e:
            logger.error(f"Error updating job counts: {e}")
    
    def _generate_document_content(self, job_id, doc_number, company_name, cin):
        """Generate simulated document content as text file"""
        document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
        doc_type = random.choice(document_types)
        
        content = f"""
COMPANY DOCUMENT - {doc_type}
========================================

Company Name: {company_name}
Corporate Identity Number (CIN): {cin}
Document Number: {doc_number}
Document Type: {doc_type}
Generated Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}

========================================
DOCUMENT CONTENT
========================================

This is a simulated {doc_type.lower()} for {company_name}.

Document Details:
- File Size: {random.randint(1024, 10240)} bytes
- Checksum: MD5-{random.randint(100000, 999999)}
- Processing Job ID: {job_id}

Content Summary:
This document contains important business information for {company_name}
registered under CIN {cin}. The document was processed as part of the
automated document download system.

========================================
Document Footer - Generated by FileSure
========================================
        """.strip()
        
        return content
    
    def _save_document_to_mongodb(self, job_id, doc_number, company_name, cin, blob_url=None):
        """Save document metadata to MongoDB documents collection"""
        try:
            document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
            doc_type = random.choice(document_types)
            
            doc_metadata = {
                "jobId": job_id,
                "documentNumber": doc_number,
                "companyName": company_name,
                "cin": cin,
                "documentType": doc_type,
                "fileName": f"document_{doc_number}.txt",
                "blobUrl": blob_url,
                "fileSize": random.randint(1024, 10240),
                "checksum": f"MD5-{random.randint(100000, 999999)}",
                "downloadedAt": datetime.utcnow(),
                "createdAt": datetime.utcnow()
            }
            
            self.docs_collection.insert_one(doc_metadata)
            logger.info(f"Saved document {doc_number} metadata to MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save document {doc_number} metadata to MongoDB: {e}")
            return False
    
    def _upload_document_to_blob(self, job_id, doc_number, company_name, cin):
        """Upload simulated document to Azure Blob Storage"""
        if not self.blob_service_client:
            logger.warning("Azure Blob Storage not configured, skipping upload")
            return None
        
        try:
            start_time = time.time()
            
            # Generate document content
            document_content = self._generate_document_content(job_id, doc_number, company_name, cin)
            
            # Create blob name
            blob_name = f"jobs/{job_id}/document_{doc_number}.txt"
            
            # Upload to blob storage
            blob_client = self.blob_service_client.get_blob_client(
                container=self.blob_container, 
                blob=blob_name
            )
            
            blob_client.upload_blob(document_content, overwrite=True)
            
            # Track metrics
            upload_time = time.time() - start_time
            self.blob_operations.labels(operation='upload').inc()
            self.blob_operation_time.labels(operation='upload').observe(upload_time)
            self.documents_uploaded.inc()
            
            # Get blob URL
            blob_url = blob_client.url
            
            logger.info(f"Uploaded document {doc_number} to blob storage: {blob_name}")
            return blob_url
            
        except Exception as e:
            logger.error(f"Failed to upload document {doc_number} to blob storage: {e}")
            return None
    
    def process_job(self, job):
        """Process a single job by simulating document download"""
        job_id = job["_id"]
        cin = job.get("cin", "Unknown")
        company_name = job.get("companyName", "Unknown")
        
        logger.info(f"Processing job {job_id} for {company_name} (CIN: {cin})")
        
        # Track processing time
        start_time = time.time()
        
        try:
            # Check if this is the first run (totalDocuments is 0)
            current_job = self.collection.find_one({"_id": job_id})
            current_total = current_job.get("processingStages", {}).get("documentDownload", {}).get("totalDocuments", 0)
            current_downloaded = current_job.get("processingStages", {}).get("documentDownload", {}).get("downloadedDocuments", 0)
            
            # Verify we still have the lock and it hasn't expired
            if current_job.get("jobStatus") != "processing":
                logger.warning(f"Lost lock on job {job_id}, skipping")
                return False
            
            # Check if lock has expired (backup check)
            locked_at = current_job.get("lockedAt")
            if locked_at:
                lock_age = (datetime.utcnow() - locked_at).total_seconds()
                if lock_age > 600:  # 10 minutes
                    logger.warning(f"Lock expired on job {job_id} (age: {lock_age:.1f}s), skipping")
                    return False
            
            # If first run, set random total documents between 30-50
            if current_total == 0:
                total_documents = random.randint(30, 50)
                logger.info(f"First run: Setting total documents to {total_documents}")
                
                # Update job with total documents
                self.collection.update_one(
                    {"_id": job_id},
                    {
                        "$set": {
                            "jobStatus": "in_progress",
                            "processingStages.documentDownload.status": "in_progress",
                            "processingStages.documentDownload.totalDocuments": total_documents,
                            "processingStages.documentDownload.downloadedDocuments": 0,
                            "processingStages.documentDownload.pendingDocuments": total_documents,
                            "processingStages.documentDownload.lastUpdated": datetime.utcnow(),
                            "updatedAt": datetime.utcnow()
                        }
                    }
                )
            else:
                total_documents = current_total
                logger.info(f"Subsequent run: Total documents is {total_documents}, already downloaded {current_downloaded}")
            
            # Calculate how many documents to download in this run (35% of total)
            docs_to_download = max(1, int(total_documents * 0.35))
            
            # Ensure we don't download more than remaining
            remaining_docs = total_documents - current_downloaded
            docs_to_download = min(docs_to_download, remaining_docs)
            
            # Track batch size
            self.download_batch_size.observe(docs_to_download)
            
            logger.info(f"Will download {docs_to_download} documents in this run (35% of {total_documents} total)")
            
            # Simulate downloading documents one by one
            for i in range(docs_to_download):
                doc_num = current_downloaded + i + 1
                # Simulate download time (1 second per document)
                time.sleep(1)
                
                downloaded_count = current_downloaded + i + 1
                pending_count = total_documents - downloaded_count
                
                logger.info(f"Downloaded document {doc_num}/{total_documents} for {company_name}")
                
                # Track document download
                self.documents_downloaded.inc()
                
                # Upload document to Azure Blob Storage
                blob_url = self._upload_document_to_blob(job_id, doc_num, company_name, cin)
                
                # Save document metadata to MongoDB
                doc_saved = self._save_document_to_mongodb(job_id, doc_num, company_name, cin, blob_url)
                if not doc_saved:
                    logger.warning(f"Failed to save document {doc_num} metadata to MongoDB")
                
                # Update progress
                self.collection.update_one(
                    {"_id": job_id},
                    {
                        "$set": {
                            "processingStages.documentDownload.downloadedDocuments": downloaded_count,
                            "processingStages.documentDownload.pendingDocuments": pending_count,
                            "processingStages.documentDownload.lastUpdated": datetime.utcnow(),
                            "updatedAt": datetime.utcnow()
                        }
                    }
                )
            
            # Check if job is completed
            final_downloaded = current_downloaded + docs_to_download
            if final_downloaded >= total_documents:
                # Mark job as completed and release lock
                self.collection.update_one(
                    {"_id": job_id},
                    {
                        "$set": {
                            "jobStatus": "completed",
                            "processingStages.documentDownload.status": "completed",
                            "processingStages.documentDownload.downloadedDocuments": total_documents,
                            "processingStages.documentDownload.pendingDocuments": 0,
                            "processingStages.documentDownload.lastUpdated": datetime.utcnow(),
                            "updatedAt": datetime.utcnow()
                        },
                        "$unset": {
                            "lockedBy": "",
                            "lockedAt": ""
                        }
                    }
                )
                self.jobs_processed.labels(status='completed').inc()
                logger.info(f"Job completed! Downloaded all {total_documents} documents for {company_name}")
            else:
                # Mark job back to pending for next run and release lock
                self.collection.update_one(
                    {"_id": job_id},
                    {
                        "$set": {
                            "jobStatus": "pending",
                            "processingStages.documentDownload.status": "pending",
                            "processingStages.documentDownload.lastUpdated": datetime.utcnow(),
                            "updatedAt": datetime.utcnow()
                        },
                        "$unset": {
                            "lockedBy": "",
                            "lockedAt": ""
                        }
                    }
                )
                self.jobs_processed.labels(status='pending').inc()
                logger.info(f"Run completed. Downloaded {final_downloaded}/{total_documents} documents for {company_name}. Job set back to pending for next run.")
            
            # Track processing time
            processing_time = time.time() - start_time
            self.processing_time.observe(processing_time)
            
            logger.info(f"Successfully completed job {job_id} for {company_name}")
            return True
            
        except Exception as e:
            # Track processing time even for failed jobs
            processing_time = time.time() - start_time
            self.processing_time.observe(processing_time)
            
            logger.error(f"Error processing job {job_id}: {e}")
            self.jobs_failed.inc()
            
            # Mark job as failed and release lock
            self.collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "jobStatus": "failed",
                        "processingStages.documentDownload.status": "failed",
                        "processingStages.documentDownload.lastUpdated": datetime.utcnow(),
                        "updatedAt": datetime.utcnow()
                    },
                    "$unset": {
                        "lockedBy": "",
                        "lockedAt": ""
                    }
                }
            )
            return False
    
    def run(self, job_id):
        """Process a single job by ID and exit"""
        logger.info(f"Starting Document Downloader for job {job_id}...")
        
        try:
            # Get the specific job
            job = self.get_job_by_id(job_id)
            
            if not job:
                logger.error(f"Could not acquire job {job_id}. Exiting.")
                sys.exit(1)
            
            # Process the job
            success = self.process_job(job)
            
            if success:
                logger.info(f"Successfully processed job {job_id}. Exiting.")
                sys.exit(0)
            else:
                logger.error(f"Failed to process job {job_id}. Exiting.")
                sys.exit(1)
                
        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Exiting.")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error processing job {job_id}: {e}")
            sys.exit(1)

# Add metrics endpoint for worker
app = Flask(__name__)

@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

def start_metrics_server():
    app.run(host="0.0.0.0", port=9100)

if __name__ == "__main__":
    # Get job ID from command line argument or environment variable
    job_id = None
    
    if len(sys.argv) == 2:
        job_id = sys.argv[1]
    else:
        job_id = os.environ.get("JOB_ID")
    
    if not job_id:
        print("Usage: python downloader.py <job_id>")
        print("Or set JOB_ID environment variable")
        print("Example: python downloader.py 68a6b73df6d117b0aeaa695e")
        print("Example: JOB_ID=68a6b73df6d117b0aeaa695e python downloader.py")
        sys.exit(1)
    
    # Start metrics server in a separate thread
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()
    
    downloader = DocumentDownloader()
    downloader.run(job_id)
