import os
import time
import random
import sys
import threading
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from bson import ObjectId
import logging
from prometheus_client import Counter, Summary, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST, CollectorRegistry, push_to_gateway
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DocumentDownloader:
    def __init__(self):
        # Validate environment variables
        required_envs = ["MONGO_URI", "AWS_REGION", "AWS_BUCKET_NAME"]
        for env in required_envs:
            if not os.environ.get(env):
                logger.error(f"Missing required environment variable: {env}")
                sys.exit(1)

        # MongoDB setup
        self.mongo_uri = os.environ.get("MONGO_URI")
        self.db_name = "filesure"
        self.collection_name = "jobs"
        self.docs_collection_name = "documents"
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            self.docs_collection = self.db[self.docs_collection_name]
            self.collection.create_index("lockedAt")
            self.collection.create_index("jobStatus")  # Added index for KEDA trigger
            logger.info("MongoDB client initialized and collections/indexes set up")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB: {e}")
            sys.exit(1)

        # AWS S3 setup
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.aws_region = os.environ.get("AWS_REGION", "ap-south-1")
        self.aws_bucket = os.environ.get("AWS_BUCKET_NAME", "filesure-documents")
        try:
            session = boto3.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.aws_region
            )
            self.s3_client = session.client('s3')
            self.s3_client.head_bucket(Bucket=self.aws_bucket)
            logger.info(f"AWS S3 client initialized for bucket: {self.aws_bucket}")
        except NoCredentialsError:
            logger.warning("No AWS credentials provided, attempting to use IAM role")
            try:
                self.s3_client = boto3.client('s3', region_name=self.aws_region)
                self.s3_client.head_bucket(Bucket=self.aws_bucket)
                logger.info(f"AWS S3 client initialized using IAM role for bucket: {self.aws_bucket}")
            except Exception as e:
                logger.error(f"Failed to initialize AWS S3 client with IAM role: {e}")
                sys.exit(1)
        except ClientError as e:
            logger.error(f"Failed to access S3 bucket {self.aws_bucket}: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error initializing AWS S3 client: {e}")
            sys.exit(1)

        # Prometheus metrics
        self.registry = CollectorRegistry()
        self.jobs_processed = Counter('jobs_processed_total', 'Total number of jobs processed', ['status'], registry=self.registry)
        self.jobs_found = Counter('jobs_found_total', 'Total number of jobs found in each cycle', registry=self.registry)
        self.jobs_locked = Counter('jobs_locked_total', 'Total number of jobs successfully locked', registry=self.registry)
        self.jobs_failed = Counter('jobs_failed_total', 'Total number of jobs that failed processing', registry=self.registry)
        self.jobs_skipped = Counter('jobs_skipped_total', 'Total number of jobs skipped due to lock issues', registry=self.registry)
        self.documents_downloaded = Counter('documents_downloaded_total', 'Total number of documents downloaded', registry=self.registry)
        self.documents_uploaded = Counter('documents_uploaded_total', 'Total number of documents uploaded to S3', registry=self.registry)
        self.s3_operations = Counter('s3_operations_total', 'Total S3 operations', ['operation'], registry=self.registry)
        self.s3_operation_time = Summary('s3_operation_duration_seconds', 'Time spent on S3 operations', ['operation'], registry=self.registry)
        self.processing_time = Summary('job_processing_duration_seconds', 'Time spent processing jobs', registry=self.registry)
        self.lock_cleanup = Counter('lock_cleanup_total', 'Total number of expired locks cleaned up', registry=self.registry)
        self.active_jobs = Gauge('active_jobs', 'Number of jobs currently being processed', registry=self.registry)
        self.pending_jobs = Gauge('pending_jobs', 'Number of jobs waiting to be processed', registry=self.registry)
        self.completed_jobs = Gauge('completed_jobs', 'Number of jobs completed', registry=self.registry)
        self.download_batch_size = Histogram('download_batch_size', 'Number of documents downloaded per batch', registry=self.registry)

        logger.info("Document Downloader initialized")

    def get_pending_job(self):
        """Find and lock a pending job atomically"""
        try:
            now = datetime.now(timezone.utc)
            job = self.collection.find_one_and_update(
                {
                    "jobStatus": "pending",
                    "$or": [
                        {"lockedAt": {"$exists": False}},
                        {"lockedAt": {"$lt": now - timedelta(minutes=10)}}
                    ]
                },
                {
                    "$set": {
                        "jobStatus": "processing",
                        "processingStages.documentDownload.status": "processing",
                        "processingStages.documentDownload.lastUpdated": now,
                        "updatedAt": now,
                        "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
                        "lockedAt": now
                    }
                },
                return_document=True
            )
            if job:
                self.jobs_locked.inc()
                self.active_jobs.inc()
                logger.info(f"Acquired lock on pending job {job['_id']} for {job.get('companyName', 'Unknown')}")
                return job
            else:
                pending_count = self.collection.count_documents({"jobStatus": "pending"})
                self.jobs_found.inc(pending_count)
                logger.info(f"No pending jobs available to lock. Current pending job count: {pending_count}")
                return None
        except Exception as e:
            logger.error(f"Error acquiring pending job: {e}")
            return None

    def get_job_by_id(self, job_id):
        """Get a specific job by ID and try to acquire lock"""
        try:
            object_id = ObjectId(job_id)
            job = self.collection.find_one_and_update(
                {"_id": object_id, "jobStatus": "pending"},
                {
                    "$set": {
                        "jobStatus": "processing",
                        "processingStages.documentDownload.status": "processing",
                        "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
                        "updatedAt": datetime.now(timezone.utc),
                        "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
                        "lockedAt": datetime.now(timezone.utc)
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
            cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=10)
            result = self.collection.update_many(
                {
                    "jobStatus": "processing",
                    "lockedAt": {"$lt": cutoff_time}
                },
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
            if result.modified_count > 0:
                self.lock_cleanup.inc(result.modified_count)
                expired_jobs = self.collection.find(
                    {"jobStatus": "pending", "lockedAt": {"$exists": False}}
                )
                job_ids = [str(job["_id"]) for job in expired_jobs]
                logger.info(f"Cleaned up {result.modified_count} expired locks for jobs: {job_ids}")
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
Generated Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

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
        for attempt in range(3):  # Retry up to 3 times
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
                    "downloadedAt": datetime.now(timezone.utc),
                    "createdAt": datetime.now(timezone.utc)
                }
                self.docs_collection.insert_one(doc_metadata)
                logger.info(f"Saved document {doc_number} metadata to MongoDB")
                return True
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed to save document {doc_number} metadata to MongoDB: {e}")
                if attempt == 2:
                    logger.error(f"Failed to save document {doc_number} metadata after 3 attempts: {e}")
                    return False
                time.sleep(1)  # Wait before retry
        return False

    def _upload_document_to_s3(self, job_id, doc_number, company_name, cin):
        """Upload simulated document to AWS S3"""
        if not self.s3_client:
            logger.warning("AWS S3 client not configured, skipping upload")
            return None
        for attempt in range(3):  # Retry up to 3 times
            try:
                start_time = time.time()
                document_content = self._generate_document_content(job_id, doc_number, company_name, cin)
                key = f"jobs/{job_id}/document_{doc_number}.txt"
                self.s3_client.put_object(
                    Bucket=self.aws_bucket,
                    Key=key,
                    Body=document_content.encode('utf-8')
                )
                blob_url = f"https://{self.aws_bucket}.s3.{self.aws_region}.amazonaws.com/{key}"
                upload_time = time.time() - start_time
                self.s3_operations.labels(operation='upload').inc()
                self.s3_operation_time.labels(operation='upload').observe(upload_time)
                self.documents_uploaded.inc()
                logger.info(f"Uploaded document {doc_number} to S3: {key}")
                return blob_url
            except ClientError as e:
                logger.warning(f"Attempt {attempt + 1} failed to upload document {doc_number} to S3: {e}")
                if attempt == 2:
                    logger.error(f"Failed to upload document {doc_number} to S3 after 3 attempts: {e}")
                    return None
                time.sleep(1)  # Wait before retry
            except Exception as e:
                logger.error(f"Unexpected error uploading document {doc_number} to S3: {e}")
                return None
        return None

    def process_job(self, job):
        """Process a single job by simulating document download"""
        job_id = job["_id"]
        cin = job.get("cin", "Unknown")
        company_name = job.get("companyName", "Unknown")
        logger.info(f"Processing job {job_id} for {company_name} (CIN: {cin})")
        start_time = time.time()
        try:
            # Verify job lock
            current_job = self.collection.find_one({"_id": job_id})
            if current_job.get("jobStatus") != "processing":
                logger.warning(f"Lost lock on job {job_id}, skipping")
                self.jobs_skipped.inc()
                self.active_jobs.dec()
                return False
            locked_at = current_job.get("lockedAt")
            if locked_at:
                lock_age = (datetime.now(timezone.utc) - locked_at.replace(tzinfo=timezone.utc)).total_seconds()
                if lock_age > 600:
                    logger.warning(f"Lock expired on job {job_id} (age: {lock_age:.1f}s), resetting to pending")
                    self.jobs_skipped.inc()
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
                    self.active_jobs.dec()
                    return False

            # Initialize or retrieve document counts
            current_total = current_job.get("processingStages", {}).get("documentDownload", {}).get("totalDocuments", 0)
            current_downloaded = current_job.get("processingStages", {}).get("documentDownload", {}).get("downloadedDocuments", 0)
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

            # Process batch of documents
            docs_to_download = max(1, int(total_documents * 0.35))
            remaining_docs = total_documents - current_downloaded
            docs_to_download = min(docs_to_download, remaining_docs)
            self.download_batch_size.observe(docs_to_download)
            logger.info(f"Will download {docs_to_download} documents in this run (35% of {total_documents} total)")

            for i in range(docs_to_download):
                doc_num = current_downloaded + i + 1
                time.sleep(1)  # Simulate download
                downloaded_count = current_downloaded + i + 1
                pending_count = total_documents - downloaded_count
                logger.info(f"Downloaded document {doc_num}/{total_documents} for {company_name}")
                self.documents_downloaded.inc()
                blob_url = self._upload_document_to_s3(job_id, doc_num, company_name, cin)
                doc_saved = self._save_document_to_mongodb(job_id, doc_num, company_name, cin, blob_url)
                if not doc_saved:
                    logger.warning(f"Failed to save document {doc_num} metadata to MongoDB")

                # Update document counts incrementally
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

            # Final status update
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
                            "processingStages.documentDownload.downloadedDocuments": final_downloaded,
                            "processingStages.documentDownload.pendingDocuments": total_documents - final_downloaded,
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
            self.active_jobs.dec()
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
                        "updatedAt": datetime.now(timezone.utc),
                        "errorMessage": str(e)  # Add error message for debugging
                    },
                    "$unset": {
                        "lockedBy": "",
                        "lockedAt": ""
                    }
                }
            )
            self.active_jobs.dec()
            return False
        finally:
            self._update_job_counts()
            for attempt in range(3):  # Retry up to 3 times
                try:
                    push_to_gateway('pushgateway.filesure.svc.cluster.local:9091', job=f'worker_{job_id}', registry=self.registry)
                    logger.info(f"Pushed metrics for job {job_id} to Pushgateway")
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt + 1} failed to push metrics for job {job_id}: {e}")
                    if attempt == 2:
                        logger.error(f"Failed to push metrics for job {job_id} after 3 attempts: {e}")
                    time.sleep(1)  # Wait before retry

    def run(self):
        """Process a pending job and exit"""
        logger.info("Starting Document Downloader...")
        try:
            self._cleanup_expired_locks()
            self._update_job_counts()
            job = self.get_pending_job()
            if not job:
                pending_count = self.collection.count_documents({"jobStatus": "pending"})
                logger.info(f"No pending job found. Current pending job count: {pending_count}")
                sys.exit(0)
            success = self.process_job(job)
            if success:
                logger.info(f"Successfully processed job {job['_id']}. Exiting.")
                sys.exit(0)
            else:
                logger.error(f"Failed to process job {job['_id']}. Exiting.")
                sys.exit(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Exiting.")
            self.active_jobs.dec()  # Ensure cleanup on interrupt
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error in run loop: {e}")
            sys.exit(1)

# Flask app for Prometheus metrics
app = Flask(__name__)

@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

def start_metrics_server():
    app.run(host="0.0.0.0", port=9100)

if __name__ == "__main__":
    metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
    metrics_thread.start()
    downloader = DocumentDownloader()
    downloader.run()


# import os
# import time
# import random
# import sys
# import threading
# from datetime import datetime, timedelta, timezone
# from pymongo import MongoClient
# from bson import ObjectId
# import logging
# from prometheus_client import Counter, Summary, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
# import boto3
# from botocore.exceptions import NoCredentialsError, ClientError
# from flask import Flask
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# class DocumentDownloader:
#     def __init__(self):
#         # MongoDB setup
#         self.mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
#         self.db_name = "filesure"
#         self.collection_name = "jobs"
#         self.docs_collection_name = "documents"
#         try:
#             self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
#             self.db = self.client[self.db_name]
#             self.collection = self.db[self.collection_name]
#             self.docs_collection = self.db[self.docs_collection_name]
#             self.collection.create_index("lockedAt")
#             self.collection.create_index("jobStatus")  # Added index for KEDA trigger
#             logger.info("MongoDB client initialized and collections/indexes set up")
#         except Exception as e:
#             logger.error(f"Failed to initialize MongoDB: {e}")
#             sys.exit(1)

#         # AWS S3 setup
#         self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
#         self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
#         self.aws_region = os.environ.get("AWS_REGION", "ap-south-1")
#         self.aws_bucket = os.environ.get("AWS_BUCKET_NAME", "filesure-documents")
#         try:
#             session = boto3.Session(
#                 aws_access_key_id=self.aws_access_key_id,
#                 aws_secret_access_key=self.aws_secret_access_key,
#                 region_name=self.aws_region
#             )
#             self.s3_client = session.client('s3')
#             self.s3_client.head_bucket(Bucket=self.aws_bucket)
#             logger.info(f"AWS S3 client initialized for bucket: {self.aws_bucket}")
#         except NoCredentialsError:
#             logger.warning("No AWS credentials provided, attempting to use IAM role")
#             try:
#                 self.s3_client = boto3.client('s3', region_name=self.aws_region)
#                 self.s3_client.head_bucket(Bucket=self.aws_bucket)
#                 logger.info(f"AWS S3 client initialized using IAM role for bucket: {self.aws_bucket}")
#             except Exception as e:
#                 logger.error(f"Failed to initialize AWS S3 client with IAM role: {e}")
#                 sys.exit(1)
#         except ClientError as e:
#             logger.error(f"Failed to access S3 bucket {self.aws_bucket}: {e}")
#             sys.exit(1)
#         except Exception as e:
#             logger.error(f"Unexpected error initializing AWS S3 client: {e}")
#             sys.exit(1)

#         # Prometheus metrics
#         self.jobs_processed = Counter('jobs_processed_total', 'Total number of jobs processed', ['status'])
#         self.jobs_found = Counter('jobs_found_total', 'Total number of jobs found in each cycle')
#         self.jobs_locked = Counter('jobs_locked_total', 'Total number of jobs successfully locked')
#         self.jobs_failed = Counter('jobs_failed_total', 'Total number of jobs that failed processing')
#         self.jobs_skipped = Counter('jobs_skipped_total', 'Total number of jobs skipped due to lock issues')  # New metric
#         self.documents_downloaded = Counter('documents_downloaded_total', 'Total number of documents downloaded')
#         self.documents_uploaded = Counter('documents_uploaded_total', 'Total number of documents uploaded to S3')
#         self.s3_operations = Counter('s3_operations_total', 'Total S3 operations', ['operation'])
#         self.s3_operation_time = Summary('s3_operation_duration_seconds', 'Time spent on S3 operations', ['operation'])
#         self.processing_time = Summary('job_processing_duration_seconds', 'Time spent processing jobs')
#         self.lock_cleanup = Counter('lock_cleanup_total', 'Total number of expired locks cleaned up')
#         self.active_jobs = Gauge('active_jobs', 'Number of jobs currently being processed')
#         self.pending_jobs = Gauge('pending_jobs', 'Number of jobs waiting to be processed')
#         self.completed_jobs = Gauge('completed_jobs', 'Number of jobs completed')
#         self.download_batch_size = Histogram('download_batch_size', 'Number of documents downloaded per batch')

#         logger.info("Document Downloader initialized")

#     def get_pending_job(self):
#         """Find and lock a pending job atomically"""
#         try:
#             now = datetime.now(timezone.utc)
#             job = self.collection.find_one_and_update(
#                 {
#                     "jobStatus": "pending",
#                     "$or": [
#                         {"lockedAt": {"$exists": False}},
#                         {"lockedAt": {"$lt": now - timedelta(minutes=10)}}
#                     ]
#                 },
#                 {
#                     "$set": {
#                         "jobStatus": "processing",
#                         "processingStages.documentDownload.status": "processing",
#                         "processingStages.documentDownload.lastUpdated": now,
#                         "updatedAt": now,
#                         "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
#                         "lockedAt": now
#                     }
#                 },
#                 return_document=True
#             )
#             if job:
#                 self.jobs_locked.inc()
#                 self.active_jobs.inc()
#                 logger.info(f"Acquired lock on pending job {job['_id']} for {job.get('companyName', 'Unknown')}")
#                 return job
#             else:
#                 pending_count = self.collection.count_documents({"jobStatus": "pending"})
#                 self.jobs_found.inc(pending_count)
#                 logger.info(f"No pending jobs available to lock. Current pending job count: {pending_count}")
#                 return None
#         except Exception as e:
#             logger.error(f"Error acquiring pending job: {e}")
#             return None

#     def get_job_by_id(self, job_id):
#         """Get a specific job by ID and try to acquire lock"""
#         try:
#             object_id = ObjectId(job_id)
#             job = self.collection.find_one_and_update(
#                 {"_id": object_id, "jobStatus": "pending"},
#                 {
#                     "$set": {
#                         "jobStatus": "processing",
#                         "processingStages.documentDownload.status": "processing",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc),
#                         "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
#                         "lockedAt": datetime.now(timezone.utc)
#                     }
#                 },
#                 return_document=True
#             )
#             if job:
#                 self.jobs_locked.inc()
#                 self.active_jobs.inc()
#                 logger.info(f"Acquired lock on job {job['_id']} for {job.get('companyName', 'Unknown')}")
#                 return job
#             else:
#                 logger.warning(f"Job {job_id} not found or not in pending status")
#                 return None
#         except Exception as e:
#             logger.error(f"Error getting job {job_id}: {e}")
#             return None

#     def _cleanup_expired_locks(self):
#         """Clean up locks that are older than 10 minutes"""
#         try:
#             cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=10)
#             result = self.collection.update_many(
#                 {
#                     "jobStatus": "processing",
#                     "lockedAt": {"$lt": cutoff_time}
#                 },
#                 {
#                     "$set": {
#                         "jobStatus": "pending",
#                         "processingStages.documentDownload.status": "pending",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc)
#                     },
#                     "$unset": {
#                         "lockedBy": "",
#                         "lockedAt": ""
#                     }
#                 }
#             )
#             if result.modified_count > 0:
#                 self.lock_cleanup.inc(result.modified_count)
#                 expired_jobs = self.collection.find(
#                     {"jobStatus": "pending", "lockedAt": {"$exists": False}}
#                 )
#                 job_ids = [str(job["_id"]) for job in expired_jobs]
#                 logger.info(f"Cleaned up {result.modified_count} expired locks for jobs: {job_ids}")
#         except Exception as e:
#             logger.error(f"Error cleaning up expired locks: {e}")

#     def _update_job_counts(self):
#         """Update job count metrics"""
#         try:
#             pending_count = self.collection.count_documents({"jobStatus": "pending"})
#             active_count = self.collection.count_documents({"jobStatus": "processing"})
#             completed_count = self.collection.count_documents({"jobStatus": "completed"})
#             self.pending_jobs.set(pending_count)
#             self.active_jobs.set(active_count)
#             self.completed_jobs.set(completed_count)
#         except Exception as e:
#             logger.error(f"Error updating job counts: {e}")

#     def _generate_document_content(self, job_id, doc_number, company_name, cin):
#         """Generate simulated document content as text file"""
#         document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
#         doc_type = random.choice(document_types)
#         content = f"""
# COMPANY DOCUMENT - {doc_type}
# ========================================

# Company Name: {company_name}
# Corporate Identity Number (CIN): {cin}
# Document Number: {doc_number}
# Document Type: {doc_type}
# Generated Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

# ========================================
# DOCUMENT CONTENT
# ========================================

# This is a simulated {doc_type.lower()} for {company_name}.

# Document Details:
# - File Size: {random.randint(1024, 10240)} bytes
# - Checksum: MD5-{random.randint(100000, 999999)}
# - Processing Job ID: {job_id}

# Content Summary:
# This document contains important business information for {company_name}
# registered under CIN {cin}. The document was processed as part of the
# automated document download system.

# ========================================
# Document Footer - Generated by FileSure
# ========================================
#         """.strip()
#         return content

#     def _save_document_to_mongodb(self, job_id, doc_number, company_name, cin, blob_url=None):
#         """Save document metadata to MongoDB documents collection"""
#         try:
#             document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
#             doc_type = random.choice(document_types)
#             doc_metadata = {
#                 "jobId": job_id,
#                 "documentNumber": doc_number,
#                 "companyName": company_name,
#                 "cin": cin,
#                 "documentType": doc_type,
#                 "fileName": f"document_{doc_number}.txt",
#                 "blobUrl": blob_url,
#                 "fileSize": random.randint(1024, 10240),
#                 "checksum": f"MD5-{random.randint(100000, 999999)}",
#                 "downloadedAt": datetime.now(timezone.utc),
#                 "createdAt": datetime.now(timezone.utc)
#             }
#             self.docs_collection.insert_one(doc_metadata)
#             logger.info(f"Saved document {doc_number} metadata to MongoDB")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save document {doc_number} metadata to MongoDB: {e}")
#             return False

#     def _upload_document_to_s3(self, job_id, doc_number, company_name, cin):
#         """Upload simulated document to AWS S3"""
#         if not self.s3_client:
#             logger.warning("AWS S3 client not configured, skipping upload")
#             return None
#         try:
#             start_time = time.time()
#             document_content = self._generate_document_content(job_id, doc_number, company_name, cin)
#             key = f"jobs/{job_id}/document_{doc_number}.txt"
#             self.s3_client.put_object(
#                 Bucket=self.aws_bucket,
#                 Key=key,
#                 Body=document_content.encode('utf-8')
#             )
#             blob_url = f"https://{self.aws_bucket}.s3.{self.aws_region}.amazonaws.com/{key}"
#             upload_time = time.time() - start_time
#             self.s3_operations.labels(operation='upload').inc()
#             self.s3_operation_time.labels(operation='upload').observe(upload_time)
#             self.documents_uploaded.inc()
#             logger.info(f"Uploaded document {doc_number} to S3: {key}")
#             return blob_url
#         except ClientError as e:
#             logger.error(f"Failed to upload document {doc_number} to S3: {e}")
#             return None
#         except Exception as e:
#             logger.error(f"Unexpected error uploading document {doc_number} to S3: {e}")
#             return None

#     def process_job(self, job):
#         """Process a single job by simulating document download"""
#         job_id = job["_id"]
#         cin = job.get("cin", "Unknown")
#         company_name = job.get("companyName", "Unknown")
#         logger.info(f"Processing job {job_id} for {company_name} (CIN: {cin})")
#         start_time = time.time()
#         try:
#             # Verify job lock
#             current_job = self.collection.find_one({"_id": job_id})
#             if current_job.get("jobStatus") != "processing":
#                 logger.warning(f"Lost lock on job {job_id}, skipping")
#                 self.jobs_skipped.inc()
#                 self.active_jobs.dec()
#                 return False
#             locked_at = current_job.get("lockedAt")
#             if locked_at:
#                 lock_age = (datetime.now(timezone.utc) - locked_at.replace(tzinfo=timezone.utc)).total_seconds()
#                 if lock_age > 600:
#                     logger.warning(f"Lock expired on job {job_id} (age: {lock_age:.1f}s), resetting to pending")
#                     self.jobs_skipped.inc()
#                     self.collection.update_one(
#                         {"_id": job_id},
#                         {
#                             "$set": {
#                                 "jobStatus": "pending",
#                                 "processingStages.documentDownload.status": "pending",
#                                 "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                                 "updatedAt": datetime.now(timezone.utc)
#                             },
#                             "$unset": {
#                                 "lockedBy": "",
#                                 "lockedAt": ""
#                             }
#                         }
#                     )
#                     self.active_jobs.dec()
#                     return False

#             # Initialize or retrieve document counts
#             current_total = current_job.get("processingStages", {}).get("documentDownload", {}).get("totalDocuments", 0)
#             current_downloaded = current_job.get("processingStages", {}).get("documentDownload", {}).get("downloadedDocuments", 0)
#             if current_total == 0:
#                 total_documents = random.randint(30, 50)
#                 logger.info(f"First run: Setting total documents to {total_documents}")
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "in_progress",
#                             "processingStages.documentDownload.status": "in_progress",
#                             "processingStages.documentDownload.totalDocuments": total_documents,
#                             "processingStages.documentDownload.downloadedDocuments": 0,
#                             "processingStages.documentDownload.pendingDocuments": total_documents,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         }
#                     }
#                 )
#             else:
#                 total_documents = current_total
#                 logger.info(f"Subsequent run: Total documents is {total_documents}, already downloaded {current_downloaded}")

#             # Process batch of documents
#             docs_to_download = max(1, int(total_documents * 0.35))
#             remaining_docs = total_documents - current_downloaded
#             docs_to_download = min(docs_to_download, remaining_docs)
#             self.download_batch_size.observe(docs_to_download)
#             logger.info(f"Will download {docs_to_download} documents in this run (35% of {total_documents} total)")

#             for i in range(docs_to_download):
#                 doc_num = current_downloaded + i + 1
#                 time.sleep(1)  # Simulate download
#                 downloaded_count = current_downloaded + i + 1
#                 pending_count = total_documents - downloaded_count
#                 logger.info(f"Downloaded document {doc_num}/{total_documents} for {company_name}")
#                 self.documents_downloaded.inc()
#                 blob_url = self._upload_document_to_s3(job_id, doc_num, company_name, cin)
#                 doc_saved = self._save_document_to_mongodb(job_id, doc_num, company_name, cin, blob_url)
#                 if not doc_saved:
#                     logger.warning(f"Failed to save document {doc_num} metadata to MongoDB")

#                 # Update document counts incrementally
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "processingStages.documentDownload.downloadedDocuments": downloaded_count,
#                             "processingStages.documentDownload.pendingDocuments": pending_count,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         }
#                     }
#                 )

#             # Final status update
#             final_downloaded = current_downloaded + docs_to_download
#             if final_downloaded >= total_documents:
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "completed",
#                             "processingStages.documentDownload.status": "completed",
#                             "processingStages.documentDownload.downloadedDocuments": total_documents,
#                             "processingStages.documentDownload.pendingDocuments": 0,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         },
#                         "$unset": {
#                             "lockedBy": "",
#                             "lockedAt": ""
#                         }
#                     }
#                 )
#                 self.jobs_processed.labels(status='completed').inc()
#                 logger.info(f"Job completed! Downloaded all {total_documents} documents for {company_name}")
#             else:
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "pending",
#                             "processingStages.documentDownload.status": "pending",
#                             "processingStages.documentDownload.downloadedDocuments": final_downloaded,
#                             "processingStages.documentDownload.pendingDocuments": total_documents - final_downloaded,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         },
#                         "$unset": {
#                             "lockedBy": "",
#                             "lockedAt": ""
#                         }
#                     }
#                 )
#                 self.jobs_processed.labels(status='pending').inc()
#                 logger.info(f"Run completed. Downloaded {final_downloaded}/{total_documents} documents for {company_name}. Job set back to pending for next run.")

#             processing_time = time.time() - start_time
#             self.processing_time.observe(processing_time)
#             self.active_jobs.dec()
#             logger.info(f"Successfully completed job {job_id} for {company_name}")
#             return True
#         except Exception as e:
#             processing_time = time.time() - start_time
#             self.processing_time.observe(processing_time)
#             logger.error(f"Error processing job {job_id}: {e}")
#             self.jobs_failed.inc()
#             self.collection.update_one(
#                 {"_id": job_id},
#                 {
#                     "$set": {
#                         "jobStatus": "failed",
#                         "processingStages.documentDownload.status": "failed",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc),
#                         "errorMessage": str(e)  # Add error message for debugging
#                     },
#                     "$unset": {
#                         "lockedBy": "",
#                         "lockedAt": ""
#                     }
#                 }
#             )
#             self.active_jobs.dec()
#             return False
#         finally:
#             self._update_job_counts()  # Ensure job counts are updated even on failure

#     def run(self):
#         """Process a pending job and exit"""
#         logger.info("Starting Document Downloader...")
#         try:
#             self._cleanup_expired_locks()
#             self._update_job_counts()
#             job = self.get_pending_job()
#             if not job:
#                 pending_count = self.collection.count_documents({"jobStatus": "pending"})
#                 logger.info(f"No pending job found. Current pending job count: {pending_count}")
#                 sys.exit(0)
#             success = self.process_job(job)
#             if success:
#                 logger.info(f"Successfully processed job {job['_id']}. Exiting.")
#                 sys.exit(0)
#             else:
#                 logger.error(f"Failed to process job {job['_id']}. Exiting.")
#                 sys.exit(1)
#         except KeyboardInterrupt:
#             logger.info("Received interrupt signal. Exiting.")
#             sys.exit(1)
#         except Exception as e:
#             logger.error(f"Unexpected error in run loop: {e}")
#             sys.exit(1)

# # Flask app for Prometheus metrics
# app = Flask(__name__)

# @app.route("/metrics")
# def metrics():
#     return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

# def start_metrics_server():
#     app.run(host="0.0.0.0", port=9100)

# if __name__ == "__main__":
#     metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
#     metrics_thread.start()
#     downloader = DocumentDownloader()
#     downloader.run()
# import os
# import time
# import random
# import sys
# import json
# import threading
# from datetime import datetime, timedelta, timezone
# from pymongo import MongoClient
# from bson import ObjectId
# import logging
# from prometheus_client import Counter, Summary, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
# import boto3
# from botocore.exceptions import NoCredentialsError, ClientError
# from flask import Flask
# from dotenv import load_dotenv

# # Load environment variables
# load_dotenv()

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )
# logger = logging.getLogger(__name__)

# class DocumentDownloader:
#     def __init__(self):
#         # MongoDB setup
#         self.mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
#         self.db_name = "filesure"
#         self.collection_name = "jobs"
#         self.docs_collection_name = "documents"
#         try:
#             self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
#             self.db = self.client[self.db_name]
#             self.collection = self.db[self.collection_name]
#             self.docs_collection = self.db[self.docs_collection_name]
#             self.collection.create_index("lockedAt")
#             logger.info("MongoDB client initialized and collections/indexes set up")
#         except Exception as e:
#             logger.error(f"Failed to initialize MongoDB: {e}")
#             sys.exit(1)

#         # AWS S3 setup
#         self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
#         self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
#         self.aws_region = os.environ.get("AWS_REGION", "ap-south-1")
#         self.aws_bucket = os.environ.get("AWS_BUCKET_NAME", "filesure-documents")
#         try:
#             session = boto3.Session(
#                 aws_access_key_id=self.aws_access_key_id,
#                 aws_secret_access_key=self.aws_secret_access_key,
#                 region_name=self.aws_region
#             )
#             self.s3_client = session.client('s3')
#             self.s3_client.head_bucket(Bucket=self.aws_bucket)
#             logger.info(f"AWS S3 client initialized for bucket: {self.aws_bucket}")
#         except NoCredentialsError:
#             logger.warning("No AWS credentials provided, attempting to use IAM role")
#             try:
#                 self.s3_client = boto3.client('s3', region_name=self.aws_region)
#                 self.s3_client.head_bucket(Bucket=self.aws_bucket)
#                 logger.info(f"AWS S3 client initialized using IAM role for bucket: {self.aws_bucket}")
#             except Exception as e:
#                 logger.error(f"Failed to initialize AWS S3 client with IAM role: {e}")
#                 sys.exit(1)
#         except ClientError as e:
#             logger.error(f"Failed to access S3 bucket {self.aws_bucket}: {e}")
#             sys.exit(1)
#         except Exception as e:
#             logger.error(f"Unexpected error initializing AWS S3 client: {e}")
#             sys.exit(1)

#         # Prometheus metrics
#         self.jobs_processed = Counter('jobs_processed_total', 'Total number of jobs processed', ['status'])
#         self.jobs_found = Counter('jobs_found_total', 'Total number of jobs found in each cycle')
#         self.jobs_locked = Counter('jobs_locked_total', 'Total number of jobs successfully locked')
#         self.jobs_failed = Counter('jobs_failed_total', 'Total number of jobs that failed processing')
#         self.documents_downloaded = Counter('documents_downloaded_total', 'Total number of documents downloaded')
#         self.documents_uploaded = Counter('documents_uploaded_total', 'Total number of documents uploaded to S3')
#         self.s3_operations = Counter('s3_operations_total', 'Total S3 operations', ['operation'])
#         self.s3_operation_time = Summary('s3_operation_duration_seconds', 'Time spent on S3 operations', ['operation'])
#         self.processing_time = Summary('job_processing_duration_seconds', 'Time spent processing jobs')
#         self.lock_cleanup = Counter('lock_cleanup_total', 'Total number of expired locks cleaned up')
#         self.active_jobs = Gauge('active_jobs', 'Number of jobs currently being processed')
#         self.pending_jobs = Gauge('pending_jobs', 'Number of jobs waiting to be processed')
#         self.completed_jobs = Gauge('completed_jobs', 'Number of jobs completed')
#         self.download_batch_size = Histogram('download_batch_size', 'Number of documents downloaded per batch')

#         logger.info("Document Downloader initialized")

#     def get_pending_job(self):
#         """Find and lock a pending job atomically"""
#         try:
#             now = datetime.now(timezone.utc)
#             job = self.collection.find_one_and_update(
#                 {
#                     "jobStatus": "pending",
#                     "$or": [
#                         {"lockedAt": {"$exists": False}},
#                         {"lockedAt": {"$lt": now - timedelta(minutes=10)}}
#                     ]
#                 },
#                 {
#                     "$set": {
#                         "jobStatus": "processing",
#                         "processingStages.documentDownload.status": "processing",
#                         "processingStages.documentDownload.lastUpdated": now,
#                         "updatedAt": now,
#                         "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
#                         "lockedAt": now
#                     }
#                 },
#                 return_document=True
#             )
#             if job:
#                 self.jobs_locked.inc()
#                 self.active_jobs.inc()
#                 logger.info(f"Acquired lock on pending job {job['_id']} for {job.get('companyName', 'Unknown')}")
#                 return job
#             else:
#                 logger.info("No pending jobs available to lock")
#                 return None
#         except Exception as e:
#             logger.error(f"Error acquiring pending job: {e}")
#             return None

#     def get_job_by_id(self, job_id):
#         """Get a specific job by ID and try to acquire lock"""
#         try:
#             object_id = ObjectId(job_id)
#             job = self.collection.find_one_and_update(
#                 {"_id": object_id, "jobStatus": "pending"},
#                 {
#                     "$set": {
#                         "jobStatus": "processing",
#                         "processingStages.documentDownload.status": "processing",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc),
#                         "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
#                         "lockedAt": datetime.now(timezone.utc)
#                     }
#                 },
#                 return_document=True
#             )
#             if job:
#                 self.jobs_locked.inc()
#                 self.active_jobs.inc()
#                 logger.info(f"Acquired lock on job {job['_id']} for {job.get('companyName', 'Unknown')}")
#                 return job
#             else:
#                 logger.warning(f"Job {job_id} not found or not in pending status")
#                 return None
#         except Exception as e:
#             logger.error(f"Error getting job {job_id}: {e}")
#             return None

#     def _cleanup_expired_locks(self):
#         """Clean up locks that are older than 10 minutes"""
#         try:
#             cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=10)
#             result = self.collection.update_many(
#                 {
#                     "jobStatus": "processing",
#                     "lockedAt": {"$lt": cutoff_time}
#                 },
#                 {
#                     "$set": {
#                         "jobStatus": "pending",
#                         "processingStages.documentDownload.status": "pending",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc)
#                     },
#                     "$unset": {
#                         "lockedBy": "",
#                         "lockedAt": ""
#                     }
#                 }
#             )
#             if result.modified_count > 0:
#                 self.lock_cleanup.inc(result.modified_count)
#                 logger.info(f"Cleaned up {result.modified_count} expired locks")
#         except Exception as e:
#             logger.error(f"Error cleaning up expired locks: {e}")

#     def _update_job_counts(self):
#         """Update job count metrics"""
#         try:
#             pending_count = self.collection.count_documents({"jobStatus": "pending"})
#             active_count = self.collection.count_documents({"jobStatus": "processing"})
#             completed_count = self.collection.count_documents({"jobStatus": "completed"})
#             self.pending_jobs.set(pending_count)
#             self.active_jobs.set(active_count)
#             self.completed_jobs.set(completed_count)
#         except Exception as e:
#             logger.error(f"Error updating job counts: {e}")

#     def _generate_document_content(self, job_id, doc_number, company_name, cin):
#         """Generate simulated document content as text file"""
#         document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
#         doc_type = random.choice(document_types)
#         content = f"""
# COMPANY DOCUMENT - {doc_type}
# ========================================

# Company Name: {company_name}
# Corporate Identity Number (CIN): {cin}
# Document Number: {doc_number}
# Document Type: {doc_type}
# Generated Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

# ========================================
# DOCUMENT CONTENT
# ========================================

# This is a simulated {doc_type.lower()} for {company_name}.

# Document Details:
# - File Size: {random.randint(1024, 10240)} bytes
# - Checksum: MD5-{random.randint(100000, 999999)}
# - Processing Job ID: {job_id}

# Content Summary:
# This document contains important business information for {company_name}
# registered under CIN {cin}. The document was processed as part of the
# automated document download system.

# ========================================
# Document Footer - Generated by FileSure
# ========================================
#         """.strip()
#         return content

#     def _save_document_to_mongodb(self, job_id, doc_number, company_name, cin, blob_url=None):
#         """Save document metadata to MongoDB documents collection"""
#         try:
#             document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
#             doc_type = random.choice(document_types)
#             doc_metadata = {
#                 "jobId": job_id,
#                 "documentNumber": doc_number,
#                 "companyName": company_name,
#                 "cin": cin,
#                 "documentType": doc_type,
#                 "fileName": f"document_{doc_number}.txt",
#                 "blobUrl": blob_url,
#                 "fileSize": random.randint(1024, 10240),
#                 "checksum": f"MD5-{random.randint(100000, 999999)}",
#                 "downloadedAt": datetime.now(timezone.utc),
#                 "createdAt": datetime.now(timezone.utc)
#             }
#             self.docs_collection.insert_one(doc_metadata)
#             logger.info(f"Saved document {doc_number} metadata to MongoDB")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save document {doc_number} metadata to MongoDB: {e}")
#             return False

#     def _upload_document_to_s3(self, job_id, doc_number, company_name, cin):
#         """Upload simulated document to AWS S3"""
#         if not self.s3_client:
#             logger.warning("AWS S3 client not configured, skipping upload")
#             return None
#         try:
#             start_time = time.time()
#             document_content = self._generate_document_content(job_id, doc_number, company_name, cin)
#             key = f"jobs/{job_id}/document_{doc_number}.txt"
#             self.s3_client.put_object(
#                 Bucket=self.aws_bucket,
#                 Key=key,
#                 Body=document_content.encode('utf-8')
#             )
#             blob_url = f"https://{self.aws_bucket}.s3.{self.aws_region}.amazonaws.com/{key}"
#             upload_time = time.time() - start_time
#             self.s3_operations.labels(operation='upload').inc()
#             self.s3_operation_time.labels(operation='upload').observe(upload_time)
#             self.documents_uploaded.inc()
#             logger.info(f"Uploaded document {doc_number} to S3: {key}")
#             return blob_url
#         except ClientError as e:
#             logger.error(f"Failed to upload document {doc_number} to S3: {e}")
#             return None
#         except Exception as e:
#             logger.error(f"Unexpected error uploading document {doc_number} to S3: {e}")
#             return None

#     def process_job(self, job):
#         """Process a single job by simulating document download"""
#         job_id = job["_id"]
#         cin = job.get("cin", "Unknown")
#         company_name = job.get("companyName", "Unknown")
#         logger.info(f"Processing job {job_id} for {company_name} (CIN: {cin})")
#         start_time = time.time()
#         try:
#             # Verify job lock
#             current_job = self.collection.find_one({"_id": job_id})
#             if current_job.get("jobStatus") != "processing":
#                 logger.warning(f"Lost lock on job {job_id}, skipping")
#                 return False
#             locked_at = current_job.get("lockedAt")
#             if locked_at:
#                 lock_age = (datetime.now(timezone.utc) - locked_at.replace(tzinfo=timezone.utc)).total_seconds()
#                 if lock_age > 600:
#                     logger.warning(f"Lock expired on job {job_id} (age: {lock_age:.1f}s), skipping")
#                     return False

#             # Initialize or retrieve document counts
#             current_total = current_job.get("processingStages", {}).get("documentDownload", {}).get("totalDocuments", 0)
#             current_downloaded = current_job.get("processingStages", {}).get("documentDownload", {}).get("downloadedDocuments", 0)
#             if current_total == 0:
#                 total_documents = random.randint(30, 50)
#                 logger.info(f"First run: Setting total documents to {total_documents}")
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "in_progress",
#                             "processingStages.documentDownload.status": "in_progress",
#                             "processingStages.documentDownload.totalDocuments": total_documents,
#                             "processingStages.documentDownload.downloadedDocuments": 0,
#                             "processingStages.documentDownload.pendingDocuments": total_documents,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         }
#                     }
#                 )
#             else:
#                 total_documents = current_total
#                 logger.info(f"Subsequent run: Total documents is {total_documents}, already downloaded {current_downloaded}")

#             # Process batch of documents
#             docs_to_download = max(1, int(total_documents * 0.35))
#             remaining_docs = total_documents - current_downloaded
#             docs_to_download = min(docs_to_download, remaining_docs)
#             self.download_batch_size.observe(docs_to_download)
#             logger.info(f"Will download {docs_to_download} documents in this run (35% of {total_documents} total)")

#             for i in range(docs_to_download):
#                 doc_num = current_downloaded + i + 1
#                 time.sleep(1)  # Simulate download
#                 downloaded_count = current_downloaded + i + 1
#                 pending_count = total_documents - downloaded_count
#                 logger.info(f"Downloaded document {doc_num}/{total_documents} for {company_name}")
#                 self.documents_downloaded.inc()
#                 blob_url = self._upload_document_to_s3(job_id, doc_num, company_name, cin)
#                 doc_saved = self._save_document_to_mongodb(job_id, doc_num, company_name, cin, blob_url)
#                 if not doc_saved:
#                     logger.warning(f"Failed to save document {doc_num} metadata to MongoDB")

#                 # Update document counts incrementally
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "processingStages.documentDownload.downloadedDocuments": downloaded_count,
#                             "processingStages.documentDownload.pendingDocuments": pending_count,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         }
#                     }
#                 )

#             # Final status update (integrating provided snippet)
#             final_downloaded = current_downloaded + docs_to_download
#             if final_downloaded >= total_documents:
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "completed",
#                             "processingStages.documentDownload.status": "completed",
#                             "processingStages.documentDownload.downloadedDocuments": total_documents,
#                             "processingStages.documentDownload.pendingDocuments": 0,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         },
#                         "$unset": {
#                             "lockedBy": "",
#                             "lockedAt": ""
#                         }
#                     }
#                 )
#                 self.jobs_processed.labels(status='completed').inc()
#                 logger.info(f"Job completed! Downloaded all {total_documents} documents for {company_name}")
#             else:
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "pending",
#                             "processingStages.documentDownload.status": "pending",
#                             "processingStages.documentDownload.downloadedDocuments": final_downloaded,
#                             "processingStages.documentDownload.pendingDocuments": total_documents - final_downloaded,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         },
#                         "$unset": {
#                             "lockedBy": "",
#                             "lockedAt": ""
#                         }
#                     }
#                 )
#                 self.jobs_processed.labels(status='pending').inc()
#                 logger.info(f"Run completed. Downloaded {final_downloaded}/{total_documents} documents for {company_name}. Job set back to pending for next run.")

#             processing_time = time.time() - start_time
#             self.processing_time.observe(processing_time)
#             logger.info(f"Successfully completed job {job_id} for {company_name}")
#             return True
#         except Exception as e:
#             processing_time = time.time() - start_time
#             self.processing_time.observe(processing_time)
#             logger.error(f"Error processing job {job_id}: {e}")
#             self.jobs_failed.inc()
#             self.collection.update_one(
#                 {"_id": job_id},
#                 {
#                     "$set": {
#                         "jobStatus": "failed",
#                         "processingStages.documentDownload.status": "failed",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc)
#                     },
#                     "$unset": {
#                         "lockedBy": "",
#                         "lockedAt": ""
#                     }
#                 }
#             )
#             return False

#     def run(self, job_id=None):
#         """Process a single job by ID or pick a pending job, then exit"""
#         logger.info("Starting Document Downloader...")
#         try:
#             self._cleanup_expired_locks()
#             self._update_job_counts()
#             if job_id is None:
#                 job = self.get_pending_job()
#                 if not job:
#                     logger.info("No pending job found. Exiting.")
#                     sys.exit(0)
#                 job_id = str(job["_id"])
#             else:
#                 job = self.get_job_by_id(job_id)
#                 if not job:
#                     logger.error(f"Could not acquire job {job_id}. Exiting.")
#                     sys.exit(1)
#             success = self.process_job(job)
#             self._update_job_counts()
#             if success:
#                 logger.info(f"Successfully processed job {job_id}. Exiting.")
#                 sys.exit(0)
#             else:
#                 logger.error(f"Failed to process job {job_id}. Exiting.")
#                 sys.exit(1)
#         except KeyboardInterrupt:
#             logger.info("Received interrupt signal. Exiting.")
#             sys.exit(1)
#         except Exception as e:
#             logger.error(f"Unexpected error processing job {job_id}: {e}")
#             sys.exit(1)

# # Flask app for Prometheus metrics
# app = Flask(__name__)

# @app.route("/metrics")
# def metrics():
#     return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

# def start_metrics_server():
#     app.run(host="0.0.0.0", port=9100)

# if __name__ == "__main__":
#     job_id = None
#     if len(sys.argv) == 2:
#         job_id = sys.argv[1]
#     else:
#         job_id = os.environ.get("JOB_ID")
#     if job_id and not ObjectId.is_valid(job_id):
#         logger.error(f"Invalid JOB_ID: {job_id}")
#         print("Usage: python downloader.py <job_id>")
#         print("Or set JOB_ID environment variable")
#         print("Example: python downloader.py 68a6b73df6d117b0aeaa695e")
#         sys.exit(1)
#     metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
#     metrics_thread.start()
#     downloader = DocumentDownloader()
#     downloader.run(job_id)



# # import os
# # import time
# # import random
# # import sys
# # import json
# # import threading
# # from datetime import datetime, timedelta, timezone
# # from pymongo import MongoClient
# # from bson import ObjectId
# # import logging
# # from prometheus_client import Counter, Summary, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
# # import boto3
# # from botocore.exceptions import NoCredentialsError, ClientError
# # from flask import Flask
# # from dotenv import load_dotenv

# # # Load environment variables
# # load_dotenv()

# # # Configure logging
# # logging.basicConfig(
# #     level=logging.INFO,
# #     format='%(asctime)s - %(levelname)s - %(message)s'
# # )
# # logger = logging.getLogger(__name__)

# # class DocumentDownloader:
# #     def __init__(self):
# #         # MongoDB setup
# #         self.mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
# #         self.db_name = "filesure"
# #         self.collection_name = "jobs"
# #         self.docs_collection_name = "documents"
# #         try:
# #             self.client = MongoClient(self.mongo_uri)
# #             self.db = self.client[self.db_name]
# #             self.collection = self.db[self.collection_name]
# #             self.docs_collection = self.db[self.docs_collection_name]
# #             self.collection.create_index("lockedAt")
#             logger.info("MongoDB client initialized and collections/indexes set up")
#         except Exception as e:
#             logger.error(f"Failed to initialize MongoDB: {e}")
#             sys.exit(1)

#         # AWS S3 setup
#         self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
#         self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
#         self.aws_region = os.environ.get("AWS_REGION", "ap-south-1")
#         self.aws_bucket = os.environ.get("AWS_BUCKET_NAME", "filesure-documents")
#         try:
#             session = boto3.Session(
#                 aws_access_key_id=self.aws_access_key_id,
#                 aws_secret_access_key=self.aws_secret_access_key,
#                 region_name=self.aws_region
#             )
#             self.s3_client = session.client('s3')
#             self.s3_client.head_bucket(Bucket=self.aws_bucket)
#             logger.info(f"AWS S3 client initialized for bucket: {self.aws_bucket}")
#         except NoCredentialsError:
#             logger.warning("No AWS credentials provided, attempting to use IAM role")
#             try:
#                 self.s3_client = boto3.client('s3', region_name=self.aws_region)
#                 self.s3_client.head_bucket(Bucket=self.aws_bucket)
#                 logger.info(f"AWS S3 client initialized using IAM role for bucket: {self.aws_bucket}")
#             except Exception as e:
#                 logger.error(f"Failed to initialize AWS S3 client with IAM role: {e}")
#                 self.s3_client = None
#                 sys.exit(1)
#         except ClientError as e:
#             logger.error(f"Failed to access S3 bucket {self.aws_bucket}: {e}")
#             self.s3_client = None
#             sys.exit(1)
#         except Exception as e:
#             logger.error(f"Unexpected error initializing AWS S3 client: {e}")
#             self.s3_client = None
#             sys.exit(1)

#         # Prometheus metrics
#         self.jobs_processed = Counter('jobs_processed_total', 'Total number of jobs processed', ['status'])
#         self.jobs_found = Counter('jobs_found_total', 'Total number of jobs found in each cycle')
#         self.jobs_locked = Counter('jobs_locked_total', 'Total number of jobs successfully locked')
#         self.jobs_failed = Counter('jobs_failed_total', 'Total number of jobs that failed processing')
#         self.documents_downloaded = Counter('documents_downloaded_total', 'Total number of documents downloaded')
#         self.documents_uploaded = Counter('documents_uploaded_total', 'Total number of documents uploaded to S3')
#         self.s3_operations = Counter('s3_operations_total', 'Total S3 operations', ['operation'])  # Changed from blob
#         self.s3_operation_time = Summary('s3_operation_duration_seconds', 'Time spent on S3 operations', ['operation'])  # Changed from blob
#         self.processing_time = Summary('job_processing_duration_seconds', 'Time spent processing jobs')
#         self.lock_cleanup = Counter('lock_cleanup_total', 'Total number of expired locks cleaned up')
#         self.active_jobs = Gauge('active_jobs', 'Number of jobs currently being processed')
#         self.pending_jobs = Gauge('pending_jobs', 'Number of jobs waiting to be processed')
#         self.completed_jobs = Gauge('completed_jobs', 'Number of jobs completed')
#         self.download_batch_size = Histogram('download_batch_size', 'Number of documents downloaded per batch')

#         logger.info("Document Downloader initialized")

#     def get_pending_job(self):
#         """Find and lock a pending job atomically"""
#         try:
#             now = datetime.now(timezone.utc)
#             job = self.collection.find_one_and_update(
#                 {
#                     "jobStatus": "pending",
#                     "$or": [
#                         {"lockedAt": {"$exists": False}},
#                         {"lockedAt": {"$lt": now - timedelta(minutes=10)}}
#                     ]
#                 },
#                 {
#                     "$set": {
#                         "jobStatus": "processing",
#                         "processingStages.documentDownload.status": "processing",
#                         "processingStages.documentDownload.lastUpdated": now,
#                         "updatedAt": now,
#                         "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
#                         "lockedAt": now
#                     }
#                 },
#                 return_document=True
#             )
#             if job:
#                 self.jobs_locked.inc()
#                 self.active_jobs.inc()
#                 logger.info(f"Acquired lock on pending job {job['_id']} for {job.get('companyName', 'Unknown')}")
#                 return job
#             else:
#                 logger.info("No pending jobs available to lock")
#                 return None
#         except Exception as e:  # Fixed invalid syntax
#             logger.error(f"Error acquiring pending job: {e}")
#             return None

#     def get_job_by_id(self, job_id):
#         """Get a specific job by ID and try to acquire lock"""
#         try:
#             object_id = ObjectId(job_id)
#             job = self.collection.find_one_and_update(
#                 {"_id": object_id, "jobStatus": "pending"},
#                 {
#                     "$set": {
#                         "jobStatus": "processing",
#                         "processingStages.documentDownload.status": "processing",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc),
#                         "lockedBy": f"downloader-{os.getpid()}-{int(time.time())}",
#                         "lockedAt": datetime.now(timezone.utc)
#                     }
#                 },
#                 return_document=True
#             )
#             if job:
#                 self.jobs_locked.inc()
#                 self.active_jobs.inc()
#                 logger.info(f"Acquired lock on job {job['_id']} for {job.get('companyName', 'Unknown')}")
#                 return job
#             else:
#                 logger.warning(f"Job {job_id} not found or not in pending status")
#                 return None
#         except Exception as e:
#             logger.error(f"Error getting job {job_id}: {e}")
#             return None

#     def _cleanup_expired_locks(self):
#         """Clean up locks that are older than 10 minutes"""
#         try:
#             cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=10)
#             result = self.collection.update_many(
#                 {
#                     "jobStatus": "processing",
#                     "lockedAt": {"$lt": cutoff_time}
#                 },
#                 {
#                     "$set": {
#                         "jobStatus": "pending",
#                         "processingStages.documentDownload.status": "pending",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc)
#                     },
#                     "$unset": {
#                         "lockedBy": "",
#                         "lockedAt": ""
#                     }
#                 }
#             )
#             if result.modified_count > 0:
#                 self.lock_cleanup.inc(result.modified_count)
#                 logger.info(f"Cleaned up {result.modified_count} expired locks")
#         except Exception as e:
#             logger.error(f"Error cleaning up expired locks: {e}")

#     def _update_job_counts(self):
#         """Update job count metrics"""
#         try:
#             pending_count = self.collection.count_documents({"jobStatus": "pending"})
#             active_count = self.collection.count_documents({"jobStatus": "processing"})
#             completed_count = self.collection.count_documents({"jobStatus": "completed"})
#             self.pending_jobs.set(pending_count)
#             self.active_jobs.set(active_count)
#             self.completed_jobs.set(completed_count)
#         except Exception as e:
#             logger.error(f"Error updating job counts: {e}")

#     def _generate_document_content(self, job_id, doc_number, company_name, cin):
#         """Generate simulated document content as text file"""
#         document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
#         doc_type = random.choice(document_types)
#         content = f"""
# COMPANY DOCUMENT - {doc_type}
# ========================================

# Company Name: {company_name}
# Corporate Identity Number (CIN): {cin}
# Document Number: {doc_number}
# Document Type: {doc_type}
# Generated Date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}

# ========================================
# DOCUMENT CONTENT
# ========================================

# This is a simulated {doc_type.lower()} for {company_name}.

# Document Details:
# - File Size: {random.randint(1024, 10240)} bytes
# - Checksum: MD5-{random.randint(100000, 999999)}
# - Processing Job ID: {job_id}

# Content Summary:
# This document contains important business information for {company_name}
# registered under CIN {cin}. The document was processed as part of the
# automated document download system.

# ========================================
# Document Footer - Generated by FileSure
# ========================================
#         """.strip()
#         return content

#     def _save_document_to_mongodb(self, job_id, doc_number, company_name, cin, blob_url=None):
#         """Save document metadata to MongoDB documents collection"""
#         try:
#             document_types = ["Annual Report", "Financial Statement", "Compliance Document", "Board Resolution", "Tax Filing"]
#             doc_type = random.choice(document_types)
#             doc_metadata = {
#                 "jobId": job_id,
#                 "documentNumber": doc_number,
#                 "companyName": company_name,
#                 "cin": cin,
#                 "documentType": doc_type,
#                 "fileName": f"document_{doc_number}.txt",
#                 "blobUrl": blob_url,
#                 "fileSize": random.randint(1024, 10240),
#                 "checksum": f"MD5-{random.randint(100000, 999999)}",
#                 "downloadedAt": datetime.now(timezone.utc),
#                 "createdAt": datetime.now(timezone.utc)
#             }
#             self.docs_collection.insert_one(doc_metadata)
#             logger.info(f"Saved document {doc_number} metadata to MongoDB")
#             return True
#         except Exception as e:
#             logger.error(f"Failed to save document {doc_number} metadata to MongoDB: {e}")
#             return False

#     def _upload_document_to_s3(self, job_id, doc_number, company_name, cin):
#         """Upload simulated document to AWS S3"""
#         if not self.s3_client:
#             logger.warning("AWS S3 client not configured, skipping upload")
#             return None
#         try:
#             start_time = time.time()
#             document_content = self._generate_document_content(job_id, doc_number, company_name, cin)
#             key = f"jobs/{job_id}/document_{doc_number}.txt"
#             self.s3_client.put_object(
#                 Bucket=self.aws_bucket,
#                 Key=key,
#                 Body=document_content.encode('utf-8')
#             )
#             blob_url = f"https://{self.aws_bucket}.s3.{self.aws_region}.amazonaws.com/{key}"
#             upload_time = time.time() - start_time
#             self.s3_operations.labels(operation='upload').inc()  # Changed from blob
#             self.s3_operation_time.labels(operation='upload').observe(upload_time)  # Changed from blob
#             self.documents_uploaded.inc()
#             logger.info(f"Uploaded document {doc_number} to S3: {key}")
#             return blob_url
#         except ClientError as e:
#             logger.error(f"Failed to upload document {doc_number} to S3: {e}")
#             return None
#         except Exception as e:
#             logger.error(f"Unexpected error uploading document {doc_number} to S3: {e}")
#             return None

#     def process_job(self, job):
#         """Process a single job by simulating document download"""
#         job_id = job["_id"]
#         cin = job.get("cin", "Unknown")
#         company_name = job.get("companyName", "Unknown")
#         logger.info(f"Processing job {job_id} for {company_name} (CIN: {cin})")
#         start_time = time.time()
#         try:
#             current_job = self.collection.find_one({"_id": job_id})
#             current_total = current_job.get("processingStages", {}).get("documentDownload", {}).get("totalDocuments", 0)
#             current_downloaded = current_job.get("processingStages", {}).get("documentDownload", {}).get("downloadedDocuments", 0)
#             if current_job.get("jobStatus") != "processing":
#                 logger.warning(f"Lost lock on job {job_id}, skipping")
#                 return False
#             locked_at = current_job.get("lockedAt")
#             if locked_at:
#                 lock_age = (datetime.now(timezone.utc) - locked_at.replace(tzinfo=timezone.utc)).total_seconds()
#                 if lock_age > 600:
#                     logger.warning(f"Lock expired on job {job_id} (age: {lock_age:.1f}s), skipping")
#                     return False
#             if current_total == 0:
#                 total_documents = random.randint(30, 50)
#                 logger.info(f"First run: Setting total documents to {total_documents}")
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "in_progress",
#                             "processingStages.documentDownload.status": "in_progress",
#                             "processingStages.documentDownload.totalDocuments": total_documents,
#                             "processingStages.documentDownload.downloadedDocuments": 0,
#                             "processingStages.documentDownload.pendingDocuments": total_documents,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         }
#                     }
#                 )
#             else:
#                 total_documents = current_total
#                 logger.info(f"Subsequent run: Total documents is {total_documents}, already downloaded {current_downloaded}")
#             docs_to_download = max(1, int(total_documents * 0.35))
#             remaining_docs = total_documents - current_downloaded
#             docs_to_download = min(docs_to_download, remaining_docs)
#             self.download_batch_size.observe(docs_to_download)
#             logger.info(f"Will download {docs_to_download} documents in this run (35% of {total_documents} total)")
#             for i in range(docs_to_download):
#                 doc_num = current_downloaded + i + 1
#                 time.sleep(1)
#                 downloaded_count = current_downloaded + i + 1
#                 pending_count = total_documents - downloaded_count
#                 logger.info(f"Downloaded document {doc_num}/{total_documents} for {company_name}")
#                 self.documents_downloaded.inc()
#                 blob_url = self._upload_document_to_s3(job_id, doc_num, company_name, cin)
#                 doc_saved = self._save_document_to_mongodb(job_id, doc_num, company_name, cin, blob_url)
#                 if not doc_saved:
#                     logger.warning(f"Failed to save document {doc_num} metadata to MongoDB")
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "processingStages.documentDownload.downloadedDocuments": downloaded_count,
#                             "processingStages.documentDownload.pendingDocuments": pending_count,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         }
#                     }
#                 )
#             final_downloaded = current_downloaded + docs_to_download
#             if final_downloaded >= total_documents:
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "completed",
#                             "processingStages.documentDownload.status": "completed",
#                             "processingStages.documentDownload.downloadedDocuments": total_documents,
#                             "processingStages.documentDownload.pendingDocuments": 0,
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         },
#                         "$unset": {
#                             "lockedBy": "",
#                             "lockedAt": ""
#                         }
#                     }
#                 )
#                 self.jobs_processed.labels(status='completed').inc()
#                 logger.info(f"Job completed! Downloaded all {total_documents} documents for {company_name}")
#             else:
#                 self.collection.update_one(
#                     {"_id": job_id},
#                     {
#                         "$set": {
#                             "jobStatus": "pending",
#                             "processingStages.documentDownload.status": "pending",
#                             "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                             "updatedAt": datetime.now(timezone.utc)
#                         },
#                         "$unset": {
#                             "lockedBy": "",
#                             "lockedAt": ""
#                         }
#                     }
#                 )
#                 self.jobs_processed.labels(status='pending').inc()
#                 logger.info(f"Run completed. Downloaded {final_downloaded}/{total_documents} documents for {company_name}. Job set back to pending for next run.")
#             processing_time = time.time() - start_time
#             self.processing_time.observe(processing_time)
#             logger.info(f"Successfully completed job {job_id} for {company_name}")
#             return True
#         except Exception as e:
#             processing_time = time.time() - start_time
#             self.processing_time.observe(processing_time)
#             logger.error(f"Error processing job {job_id}: {e}")
#             self.jobs_failed.inc()
#             self.collection.update_one(
#                 {"_id": job_id},
#                 {
#                     "$set": {
#                         "jobStatus": "failed",
#                         "processingStages.documentDownload.status": "failed",
#                         "processingStages.documentDownload.lastUpdated": datetime.now(timezone.utc),
#                         "updatedAt": datetime.now(timezone.utc)
#                     },
#                     "$unset": {
#                         "lockedBy": "",
#                         "lockedAt": ""
#                     }
#                 }
#             )
#             return False

#     def run(self, job_id=None):
#         """Process a single job by ID or pick a pending job, then exit"""
#         logger.info("Starting Document Downloader...")
#         try:
#             self._cleanup_expired_locks()
#             self._update_job_counts()
#             if job_id is None:
#                 job = self.get_pending_job()
#                 if not job:
#                     logger.info("No pending job found. Exiting.")
#                     sys.exit(0)  # Exit gracefully if no jobs
#                 job_id = str(job["_id"])
#             else:
#                 job = self.get_job_by_id(job_id)
#                 if not job:
#                     logger.error(f"Could not acquire job {job_id}. Exiting.")
#                     sys.exit(1)
#             success = self.process_job(job)
#             self._update_job_counts()
#             if success:
#                 logger.info(f"Successfully processed job {job_id}. Exiting.")
#                 sys.exit(0)
#             else:
#                 logger.error(f"Failed to process job {job_id}. Exiting.")
#                 sys.exit(1)
#         except KeyboardInterrupt:
#             logger.info("Received interrupt signal. Exiting.")
#             sys.exit(1)
#         except Exception as e:
#             logger.error(f"Unexpected error processing job {job_id}: {e}")
#             sys.exit(1)

# # Flask app for Prometheus metrics
# app = Flask(__name__)

# @app.route("/metrics")
# def metrics():
#     return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}

# def start_metrics_server():
#     app.run(host="0.0.0.0", port=9100)

# if __name__ == "__main__":
#     # Get job ID from command line or environment variable
#     job_id = None
#     if len(sys.argv) == 2:
#         job_id = sys.argv[1]
#     else:
#         job_id = os.environ.get("JOB_ID")  # Consistent case
#     if job_id and not ObjectId.is_valid(job_id):
#         logger.error(f"Invalid JOB_ID: {job_id}")
#         print("Usage: python downloader.py <job_id>")
#         print("Or set JOB_ID environment variable")
#         print("Example: python downloader.py 68a6b73df6d117b0aeaa695e")
#         sys.exit(1)
#     metrics_thread = threading.Thread(target=start_metrics_server, daemon=True)
#     metrics_thread.start()
#     downloader = DocumentDownloader()
#     downloader.run(job_id)