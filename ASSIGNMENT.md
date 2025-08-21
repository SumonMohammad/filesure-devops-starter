# Filesure Take-Home DevOps Assignment (Intermediate Level)

## Objective

Build and deploy a simulated document processing system inspired by Filesure's real-world infrastructure. This assignment tests your ability to work with Azure cloud components, containerize services, deploy them to Kubernetes, implement CI/CD, and expose observability via Prometheus and Grafana.

## 🏠 Background (Simulated Scenario)

At Filesure, we process compliance filings submitted to India's Ministry of Corporate Affairs (MCA). These filings come in various formats and are often embedded inside scanned or structured PDF forms. To extract useful data from these documents, we have built an asynchronous document processing system.

Here's how it works in production:

- When a user requests data for a company, the system triggers a backend pipeline that automatically fetches all available ROC forms filed by that company from the MCA portal.
- Upon user's order, a job is added to the MongoDB database which will track the progress of document download.
- Jobs are picked up by containerized processes running inside Kubernetes, and the forms are downloaded and parsed.
- The extracted outputs are stored in Azure Blob Storage.
- Throughout the process, we monitor metrics like job success rates, failures, throughput, and latency via Prometheus and Grafana.

In this assignment, you'll build a simplified version of this system with two main services, infrastructure automation, and observability.

## 🛠️ Assignment Requirements

### 1. Containerization

- [ ] Create `Dockerfile` for API service
- [ ] Create `Dockerfile` for worker service
- [ ] Build and test both containers locally

### 2. Kubernetes Deployment

Write Kubernetes manifests (or Helm chart) to deploy:

- [ ] API Service (with Service and Ingress/LoadBalancer)
- [ ] Worker Service with **KEDA ScaledJob** configuration
- [ ] MongoDB (or connection to MongoDB Atlas)
- [ ] Prometheus
- [ ] Grafana
- [ ] Use Secrets/ConfigMaps for:
  - MongoDB connection string
  - Azure Blob Storage credentials

### 3. KEDA Configuration

Configure KEDA to:

- [ ] Scale based on pending jobs in MongoDB
- [ ] Pass job ID to worker container via environment variable
- [ ] Handle job completion and scaling down

### 4. Monitoring & Observability

Configure monitoring stack:

- [ ] Prometheus scraping both services (API:5001/metrics, Worker:9100/metrics)
- [ ] Grafana dashboard displaying:
  - Job creation rates
  - Job processing rates and completion times
  - Document upload metrics to Azure Blob
  - Active/pending/completed job counts
  - Error rates and failure analysis
  - Azure Blob Storage operation metrics
- [ ] **Bonus**: Set up alerts for high failure rates

### 5. CI/CD Pipeline (GitHub Actions)

Create workflow that:

- [ ] Lints and tests code (optional)
- [ ] Builds Docker images for both services
- [ ] Pushes to DockerHub/GHCR
- [ ] **Optional**: Deploys to local K8s using kubectl

## 📋 System Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Web UI        │    │   MongoDB        │    │  Azure Blob     │
│   (Port 5001)   │◄──►│   - jobs         │◄──►│   Storage       │
│                 │    │   - documents    │    │   (text files)  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                        │                       ▲
         ▼                        ▼                       │
┌─────────────────┐    ┌──────────────────┐              │
│   API Service   │    │  KEDA ScaledJob  │──────────────┘
│   /create-job   │    │  Worker Pods     │
│   /metrics      │    │  /metrics:9100   │
└─────────────────┘    └──────────────────┘
         │                        │
         ▼                        ▼
┌─────────────────┐    ┌──────────────────┐
│   Prometheus    │    │    Grafana       │
│   (Scraping)    │◄──►│   (Dashboard)    │
└─────────────────┘    └──────────────────┘
```

### Job Processing Flow

1. User creates jobs via web interface
2. Jobs stored in MongoDB `jobs` collection with `pending` status
3. KEDA detects pending jobs and triggers worker pods
4. Worker acquires lock on specific job ID
5. Worker downloads documents (35% per run) and uploads to Azure Blob
6. Document metadata saved to MongoDB `documents` collection
7. Job marked as `pending` for next run or `completed` when done
8. Worker pod exits, KEDA scales down

## 📦 Deliverables

### Required

1. **GitHub Repository** with:

   - Your Dockerfiles
   - Kubernetes manifests or Helm charts
   - CI/CD pipeline configuration
   - Updated documentation

2. **README.md** with:

   - Setup and deployment instructions
   - Azure and MongoDB prerequisites
   - How to test the system
   - Architecture decisions

3. **Grafana Dashboard**:

   - Screenshot or JSON export
   - Showing key metrics from both services

4. **Demo Video** (3-5 minutes):
   - System walkthrough
   - Job creation and processing
   - Monitoring dashboard
   - KEDA scaling demonstration

### Environment Setup

Create your own:

- **Azure Account**: Free tier provides $200 credit for Blob Storage
- **MongoDB**: Use MongoDB Atlas free tier or self-hosted
- **Kubernetes**: Local (minikube/kind) or cloud (AKS recommended)

## 🌟 Evaluation Criteria

| Area                            | Points  | Details                                            |
| ------------------------------- | ------- | -------------------------------------------------- |
| **Azure Blob Integration**      | 15      | Proper connection, document upload, error handling |
| **MongoDB-Based Job Queue**     | 15      | Job management, locking, metadata storage          |
| **Kubernetes Deployment**       | 20      | Complete K8s setup, KEDA configuration             |
| **Monitoring (Prom + Grafana)** | 20      | Comprehensive dashboards, meaningful metrics       |
| **CI/CD Pipeline**              | 15      | Automated build/deploy, proper workflow            |
| **Code Quality + Docs + Video** | 15      | Clean code, clear documentation, good demo         |
| **Total**                       | **100** |                                                    |

## 🎯 Getting Started

### 1. Environment Setup

```bash
# Clone and explore the codebase
git clone <your-repo>
cd filesure-devops-starter

# Set up environment variables
cp env.example .env
# Edit .env with your MongoDB and Azure credentials

# Test locally
cd api && python app.py
cd worker && python downloader.py <job_id>
```

### 2. Create Your Infrastructure

- Set up Azure Blob Storage container
- Configure MongoDB Atlas or self-hosted MongoDB
- Install KEDA in your Kubernetes cluster

### 3. Containerize and Deploy

- Write Dockerfiles
- Create Kubernetes manifests
- Deploy monitoring stack
- Configure KEDA ScaledJob

## ⚠️ Important Notes

- **KEDA Design**: The worker is specifically designed for KEDA ScaledJob pattern
- **Job Processing**: System processes jobs in batches (35% per run) for realistic simulation
- **Monitoring**: 15+ comprehensive Prometheus metrics across both services
- **Document Storage**: Dual storage (files in Azure Blob + metadata in MongoDB)
- **Clean Code**: Focus on production-ready patterns and observability

## 🚀 Success Tips

1. **Start Simple**: Get basic containerization working first
2. **Test Locally**: Ensure everything works before deploying to K8s
3. **Monitor Early**: Set up Prometheus/Grafana early in the process
4. **Document Decisions**: Explain your architectural choices
5. **Demo Well**: Show the complete flow from job creation to completion

Good luck! We look forward to seeing how you approach real-world DevOps challenges.

— The Filesure Team
