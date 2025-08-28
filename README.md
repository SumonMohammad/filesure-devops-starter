# FileSure DevOps Simulation

This project simulates a **document download processing system** with job queuing, processing, monitoring, and auto-scaling capabilities on **Kubernetes**.  
It demonstrates a real-world workflow where documents are downloaded in batches, stored in cloud storage, and tracked in a database with monitoring dashboards.

---

## System Architecture

### **API Service (`api/app.py`)**
- Web UI at `http://localhost:5001` for creating jobs  
- REST API to create 1–10 jobs with random company data  
- MongoDB integration for job state  
- Prometheus metrics on `/metrics`

### **Worker Service (`worker/downloader.py`)**
- Processes a single job (KEDA scaled per job)  
- Acquires job lock in MongoDB (no duplicate processing)  
- Simulates document downloads (35% per run, 30–50 total docs)  
- Uploads files to **AWS S3 (via secrets)**  
- Tracks metadata in MongoDB  
- Prometheus metrics on port `9100`

### **Monitoring & Scaling**
- **Prometheus + Grafana** dashboards for metrics  
- **KEDA ScaledJob** to auto-scale worker pods per pending job  
- Structured logs for debugging  

---

##  Key Features
- Randomized company/job data  
- Multi-run job lifecycle (`pending → processing → in_progress → completed`)  
- Batch-based document downloads  
- Dual storage (AWS S3 + MongoDB metadata)  
- Fault tolerance (lock expiry + recovery)  
- Kubernetes-native auto-scaling with KEDA  
- Monitoring with 15+ custom metrics  

---

##  Prerequisites

Before running the system, make sure you have installed:

- **[Docker](https://docs.docker.com/get-docker/)** (latest)  
- **[Kind](https://kind.sigs.k8s.io/)** (Kubernetes in Docker)  
- **[kubectl](https://kubernetes.io/docs/tasks/tools/)**  
- **[Helm](https://helm.sh/docs/intro/install/)**  
- **git**  

>  AWS credentials in the script are placeholders. Replace them with valid credentials for your S3 bucket before deployment.  

---

##  Getting Started

### 1. Clone the repository
```bash
git clone https://github.com/yourusername/filesure-devops-starter.git
cd filesure-devops-starter
```

#2. Run the create-cluster-service.sh  script

* This script sets up everything automatically:

* Creates a Kind cluster

* Installs KEDA via Helm

* Builds API + Worker Docker images

* Loads images into the Kind cluster

* Applies all Kubernetes manifests (API, worker, MongoDB, Prometheus, Grafana, exporters)

* Creates secrets for MongoDB + AWS S3

* Starts port-forwarding for services

`chmod +x create-cluster-service.sh
./create-cluster-service.sh
`
# 3. Verify cluster status

`kubectl get pods -n filesure`

* You should see API, MongoDB, Prometheus, Grafana, and worker pods running.


# Accessing the Services

* API Web UI: http://localhost:5001
* Prometheus: http://localhost:9090
* Grafana: http://localhost:3000
    * Default login: admin / admin


# Monitoring Metrics
## API (:5001/metrics)

* Request counts

* Request durations

* Database failures

* Job creation stats


## Worker (:9100/metrics)

* Jobs processed (by status)

* Lock statistics

* Documents uploaded

* Blob operation timings

* Download batch sizes


# Deployment Workflow (Automated by Script)

1. Kind cluster created with config (kubernetes/kind-config.yml)

2. KEDA installed in keda namespace

3. Docker images built and loaded into Kind cluster

4. Secrets (filesure-secrets) created for MongoDB + AWS credentials

5. Kubernetes manifests applied for API, worker, MongoDB, Prometheus, Grafana, exporters

6. Port-forwarding started (API, Grafana, Prometheus)

7. Test jobs auto-created for demonstration



# Notes

* Replace AWS credentials in create-cluster-service.sh before real use

* MongoDB connection is internal:

 `mongodb.filesure.svc.cluster.local:27017`

* Check logs with:

  `kubectl logs -n filesure <pod-name>`

  `kubectl get pods -n filesure -l scaledjob.keda.sh/name=worker-scaledjob -w`

* Tear down the cluster with:

  `kind delete cluster --name filesure-cluster`