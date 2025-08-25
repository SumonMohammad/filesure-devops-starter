import os
import time
import random
import sys
import json
import threading
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from bson import ObjectId
import logging
from prometheus_client import Counter, Summary, Histogram, generate_latest, CONTENT_TYPE_LATEST
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DocumentDownloader:
    def __init__(self):
        self.mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = "filesure"
        self.collection_name = "jobs"
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.environ.get("AWS_REGION", "ap-south-1")
        self.aws_bucket = os.environ.get("AWS_BUCKET_NAME", "filesure-documents")
        
        # Initialize MongoDB client
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info("Collections setup: collection jobs already exists")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB: {e}")
            sys.exit(1)
        
        # Initialize AWS S3 client
        try:
            if not all([self.aws_access_key_id, self.aws_secret_access_key, self.aws_region, self.aws_bucket]):
                logger.warning("AWS S3 configuration not provided")
                self.s3_client = None
            else:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.aws_region
                )
                logger.info(f"AWS S3 client initialized for bucket: {self.aws_bucket}")
        except NoCredentialsError:
            logger.error("Invalid AWS credentials")
            self.s3_client = None
            sys.exit(1)
        except Exception as e:
            logger.error(f"Failed to initialize AWS S3 client: {e}")
            self.s3_client = None
            sys.exit(1)
        
        # Prometheus metrics
        self.jobs_processed = Counter(
            'jobs_processed_total',
            'Total number of jobs processed',
            ['status']
        )
        self.jobs_failed = Counter(
            'jobs_failed_total',
            'Total number of jobs that failed'
        )
        self.documents_downloaded = Counter(
            'documents_downloaded_total',
            'Total number of documents downloaded'
        )
        self.download_batch_size = Histogram(
            'download_batch_size',
            'Number of documents downloaded per batch'
        )
        self.blob_operations = Counter(
            'blob_operations_total',
            'Total number of blob operations (upload/download)',
            ['operation']
        )
        self.processing_time = Summary(
            'job_processing_time_seconds',
            'Time spent processing jobs'
        )
        
        logger.info("Document Downloader initialized")
    
    def _upload_document_to_s3(self, job_id, doc_num, company_name, cin):
        """Simulate uploading a document to AWS S3"""
        if not self.s3_client:
            logger.error("AWS S3 client not initialized, cannot upload document")
            return None
        
        try:
            key = f"jobs/{job_id}/document_{doc_num}.txt"
            content = f"Document {doc_num} for {company_name} (CIN: {cin})"
            self.s3_client.put_object(
                Bucket=self.aws_bucket,
                Key=key,
                Body=content.encode('utf-8')
            )
            blob_url = f"https://{self.aws_bucket}.s3.{self.aws_region}.amazonaws.com/{key}"
            self.blob_operations.labels(operation='upload').inc()
            logger.info(f"Uploaded document {doc_num} to AWS S3: {key}")
            return blob_url
        except ClientError as e:
            logger.error(f"Failed to upload document {doc_num} to AWS S3: {e}")
            return None
    
    def _save_document_to_mongodb(self, job_id, doc_num, company_name, cin, blob_url):
        """Save document metadata to MongoDB"""
        try:
            document = {
                "jobId": job_id,
                "documentNumber": doc_num,
                "companyName": company_name,
                "cin": cin,
                "blobUrl": blob_url,
                "createdAt": datetime.now(timezone.utc),
                "updatedAt": datetime.now(timezone.utc)
            }
            self.db.documents.insert_one(document)
            logger.info(f"Saved document {doc_num} metadata to MongoDB")
            return True
        except Exception as e:
            logger.error(f"Failed to save document {doc_num} metadata: {e}")
            return False
    
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
                if locked_at.tzinfo is None:
                    locked_at = locked_at.replace(tzinfo=timezone.utc)
                lock_age = (datetime.now(timezone.utc) - locked_at).total_seconds()
                if lock_age > 600:  # 10 minutes
                    logger.warning(f"Lock expired on job {job_id} (age: {lock_age:.1f}s), skipping")
                    return False
            
            # If first run, set random total documents between 30-50
            if current_total == 0:
                total_documents = random.randint(30, 50)
                logger.info(f"First run: Setting total documents to {total_documents}")
                
                self.collection.update_one(
                    {"_id": job_id},
                    {
                        "$set": {
                            "jobStatus": "in_progress",
                            "processingStages.documentDownload.status": "in_progress",
                            "processingStages.documentDownload.totalDocuments": total_documents,
                            "processingStages.documentDownload.downloadedDocuments": 0,
                            "processingStages.documentDownload.pendingDocuments": total_documents,
                            "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
                            "updatedAt": datetime.now(timezone.utc)
                        }
                    }
                )
            else:
                total_documents = current_total
                logger.info(f"Subsequent run: Total documents is {total_documents}, already downloaded {current_downloaded}")
            
            # Calculate how many documents to download in this run (35% of total)
            docs_to_download = max(1, int(total_documents * 0.35))
            remaining_docs = total_documents - current_downloaded
            docs_to_download = min(docs_to_download, remaining_docs)
            
            # Track batch size
            self.download_batch_size.observe(docs_to_download)
            logger.info(f"Will download {docs_to_download} documents in this run (35% of {total_documents} total)")
            
            # Simulate downloading documents one by one
            for i in range(docs_to_download):
                doc_num = current_downloaded + i + 1
                time.sleep(1)
                downloaded_count = current_downloaded + i + 1
                pending_count = total_documents - downloaded_count
                
                logger.info(f"Downloaded document {doc_num}/{total_documents} for {company_name}")
                self.documents_downloaded.inc()
                
                # Upload document to AWS S3
                blob_url = self._upload_document_to_s3(job_id, doc_num, company_name, cin)
                
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
                            "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
                            "updatedAt": datetime.now(timezone.utc)
                        }
                    }
                )
            
            # Check if job is completed
            final_downloaded = current_downloaded + docs_to_download
            if final_downloaded >= total_documents:
                self.collection.update_one(
                    {"_id": job_id},
                    {
                        "$set": {
                            "jobStatus": "completed",
                            "processingStages.documentDownload.status": "completed",
                            "processingStages.documentDownload.downloadedDocuments": total_documents,
                            "processingStages.documentDownload.pendingDocuments": 0,
                            "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
                            "updatedAt": datetime.now(timezone.utc)
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
                self.collection.update_one(
                    {"_id": job_id},
                    {
                        "$set": {
                            "jobStatus": "pending",
                            "processingStages.documentDownload.status": "pending",
                            "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
                            "updatedAt": datetime.now(timezone.utc)
                        },
                        "$unset": {
                            "lockedBy": "",
                            "lockedAt": ""
                        }
                    }
                )
                self.jobs_processed.labels(status='pending').inc()
                logger.info(f"Run completed. Downloaded {final_downloaded}/{total_documents} documents for {company_name}. Job set back to pending for next run.")
            
            processing_time = time.time() - start_time
            self.processing_time.observe(processing_time)
            logger.info(f"Successfully completed job {job_id} for {company_name}")
            return True
            
        except Exception as e:
            processing_time = time.time() - start_time
            self.processing_time.observe(processing_time)
            logger.error(f"Error processing job {job_id}: {e}")
            self.jobs_failed.inc()
            
            self.collection.update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "jobStatus": "failed",
                        "processingStages.documentDownload.status": "failed",
                        "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
                        "updatedAt": datetime.now(timezone.utc)
                    },
                    "$unset": {
                        "lockedBy": "",
                        "lockedAt": ""
                    }
                }
            )
            return False
    
    def acquire_job(self, job_id):
        """Attempt to acquire a lock on the specified job"""
        try:
            result = self.collection.find_one_and_update(
                {
                    "_id": ObjectId(job_id),
                    "jobStatus": "pending",
                    "processingStages.documentDownload.status": "pending",
                    "$or": [
                        {"lockedBy": {"$exists": False}},
                        {"lockedBy": None},
                        {
                            "lockedAt": {
                                "$lte": datetime.now(timezone.utc) - timedelta(seconds=600)
                            }
                        }
                    ]
                },
                {
                    "$set": {
                        "jobStatus": "processing",
                        "processingStages.documentDownload.status": "processing",
                        "lockedBy": "downloader",
                        "lockedAt": datetime.now(timezone.utc),
                        "updatedAt": datetime.now(timezone.utc)
                    }
                },
                return_document=True
            )
            
            if result:
                logger.info(f"Acquired lock on job {job_id} for {result.get('companyName', 'Unknown')}")
                return result
            else:
                logger.warning(f"Job {job_id} not found or not in pending status")
                return None
        except Exception as e:
            logger.error(f"Error acquiring job {job_id}: {e}")
            return None
    
    def start(self, job_id):
        """Start the document downloader for a specific job"""
        logger.info(f"Starting Document Downloader for job {job_id}...")
        
        job = self.acquire_job(job_id)
        if not job:
            logger.error(f"Could not acquire job {job_id}. Exiting.")
            sys.exit(1)
        
        success = self.process_job(job)
        if not success:
            logger.error(f"Failed to process job {job_id}. Exiting.")
            sys.exit(1)
        
        logger.info(f"Completed processing for job {job_id}")

app = Flask(__name__)

@app.route('/metrics')
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

if __name__ == "__main__":
    # Default job ID if none provided
    default_job_id = "68aa65572f748b0201925a30"  # Replace with a valid default or generate dynamically if needed
    if len(sys.argv) == 2:
        job_id = sys.argv[1]  # User-provided job ID from command line
    else:
        job_id = os.environ.get("JOB_ID", default_job_id)  # Fallback to default if no env variable
    
    downloader = DocumentDownloader()
    
    def run_job():
        while True:
            downloader.start(job_id)
            time.sleep(10)  # Wait before retrying or checking for new jobs
    
    job_thread = threading.Thread(target=run_job, daemon=True)
    job_thread.start()
    
    logger.info(f"Starting metrics server on http://0.0.0.0:9100 with job ID: {job_id}")
    app.run(host='0.0.0.0', port=9100)
    if len(sys.argv) == 2:
        job_id = sys.argv[1]
    else:
        job_id = os.environ.get("JOB_ID")
    
    if not job_id:
        print("Usage: python downloader.py <job_id>")
        print("Or set JOB_ID environment variable")
        sys.exit(1)
    
    downloader = DocumentDownloader()
    
    def run_job():
        while True:
            downloader.start(job_id)
            time.sleep(10)  # Wait before retrying
    
    job_thread = threading.Thread(target=run_job, daemon=True)
    job_thread.start()
    
    logger.info("Starting metrics server on http://0.0.0.0:9100")
    app.run(host='0.0.0.0', port=9100)