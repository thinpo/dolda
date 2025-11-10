# 🐳☸️ Docker & Kubernetes Deployment - Quick Reference

## 📦 Files Created

### Docker Configuration
```
├── Dockerfile                   # Multi-stage build for optimal image
├── .dockerignore               # Exclude unnecessary files
├── docker-compose.yml          # 3-node cluster + monitoring
└── monitoring/
    └── prometheus.yml          # Prometheus scrape config
```

### Kubernetes Manifests
```
└── k8s/
    ├── namespace.yaml          # dolda namespace
    ├── configmap.yaml          # Environment configuration
    ├── statefulset.yaml        # 3-node StatefulSet with PVCs
    ├── service.yaml            # Headless + LoadBalancer + Metrics
    ├── servicemonitor.yaml     # Prometheus Operator integration
    ├── poddisruptionbudget.yaml # HA - min 2 nodes
    ├── networkpolicy.yaml      # Network security
    └── kustomization.yaml      # Kustomize deployment
```

### Documentation & Tools
```
├── DEPLOYMENT.md               # Comprehensive deployment guide
└── Makefile                    # Automation commands
```

---

## 🚀 Quick Start Commands

### Docker Compose (Fastest)

```bash
# Start 3-node cluster with monitoring
make compose-up
# or
docker-compose up -d

# View status
make compose-ps

# View logs
make compose-logs

# Stop cluster
make compose-down
```

**Access:**
- Node 1: http://localhost:8080 (service), http://localhost:8081 (health)
- Node 2: http://localhost:8082 (service), http://localhost:8083 (health)
- Node 3: http://localhost:8084 (service), http://localhost:8085 (health)
- Prometheus: http://localhost:9093
- Grafana: http://localhost:3000 (admin/admin)

---

### Kubernetes (Production)

```bash
# Deploy everything
make k8s-deploy
# or
kubectl apply -f k8s/

# Check status
make k8s-status

# View pods
make k8s-pods

# View logs
make k8s-logs

# Port forward for local access
make k8s-port-forward
# Access at http://localhost:8080
```

**Cleanup:**
```bash
make k8s-delete
# or
kubectl delete namespace dolda
```

---

## 🎯 Key Features

### Docker Setup

✅ **Multi-stage build** - Optimized image size  
✅ **3-node cluster** - Full Raft quorum  
✅ **Health checks** - Automatic restart on failure  
✅ **Persistent volumes** - Data survives restarts  
✅ **Monitoring stack** - Prometheus + Grafana included  
✅ **Network isolation** - Dedicated bridge network  

### Kubernetes Setup

✅ **StatefulSet** - Stable network identities  
✅ **Persistent storage** - 100Gi data + 10Gi logs per node  
✅ **High availability** - PodDisruptionBudget ensures 2+ nodes  
✅ **Auto-scaling ready** - Easy to scale up/down  
✅ **Health probes** - Liveness, readiness, startup  
✅ **Network policies** - Secure traffic control  
✅ **Service mesh ready** - Annotations for Prometheus scraping  
✅ **Anti-affinity** - Spread pods across nodes  

---

## 📊 Architecture

### Docker Compose Network
```
┌─────────────────────────────────────────────────┐
│                 dolda-network                   │
│                 (172.28.0.0/16)                 │
│                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐     │
│  │  node1   │  │  node2   │  │  node3   │     │
│  │  :8080   │  │  :8082   │  │  :8084   │     │
│  │  :7001   │◄─┼─►:7002   │◄─┼─►:7003   │     │
│  └──────────┘  └──────────┘  └──────────┘     │
│       │              │              │          │
│       └──────────┬───┴──────────────┘          │
│                  │                             │
│         ┌────────▼────────┐                    │
│         │   Prometheus    │                    │
│         │     :9093       │                    │
│         └────────┬────────┘                    │
│                  │                             │
│         ┌────────▼────────┐                    │
│         │    Grafana      │                    │
│         │     :3000       │                    │
│         └─────────────────┘                    │
└─────────────────────────────────────────────────┘
```

### Kubernetes Architecture
```
┌─────────────────────────────────────────────────┐
│               Namespace: dolda                  │
│                                                 │
│  ┌──────────────────────────────────────────┐  │
│  │         dolda-service (LoadBalancer)     │  │
│  │               External IP                │  │
│  └──────────────────┬───────────────────────┘  │
│                     │                          │
│  ┌──────────────────▼───────────────────────┐  │
│  │      dolda-headless (Headless Service)   │  │
│  └──────────────────┬───────────────────────┘  │
│                     │                          │
│       ┌─────────────┼─────────────┐            │
│       │             │             │            │
│  ┌────▼────┐  ┌────▼────┐  ┌────▼────┐       │
│  │ dolda-0 │  │ dolda-1 │  │ dolda-2 │       │
│  │  Pod    │◄─┤  Pod    │◄─┤  Pod    │       │
│  │ [PVC]   │  │ [PVC]   │  │ [PVC]   │       │
│  └─────────┘  └─────────┘  └─────────┘       │
│                                                 │
│  ┌──────────────────────────────────────────┐  │
│  │        PodDisruptionBudget: min 2        │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
         │
         ▼ (Prometheus scraping via ServiceMonitor)
┌─────────────────────────────────────────────────┐
│          Monitoring Namespace                   │
│  ┌──────────────┐      ┌──────────────┐        │
│  │  Prometheus  │─────►│   Grafana    │        │
│  └──────────────┘      └──────────────┘        │
└─────────────────────────────────────────────────┘
```

---

## 🔧 Configuration

### Docker Environment Variables

Set in `docker-compose.yml`:
```yaml
environment:
  - DOLDA_NODE_ID=1
  - DOLDA_NODE_NAME=node1
  - DOLDA_PEERS=dolda-node2:7000,dolda-node3:7000
  - DOLDA_DATA_DIR=/data
  - RUST_LOG=info
```

### Kubernetes ConfigMap

Edit `k8s/configmap.yaml`:
```yaml
data:
  DOLDA_LOG_LEVEL: "info"
  DOLDA_SEGMENT_SIZE_MB: "256"
  DOLDA_MAX_SEGMENTS: "1000"
```

---

## 📈 Monitoring

### Metrics Endpoints

**Docker Compose:**
- Node 1: http://localhost:9090/metrics
- Node 2: http://localhost:9091/metrics
- Node 3: http://localhost:9092/metrics

**Kubernetes:**
```bash
kubectl port-forward -n dolda dolda-0 9090:9090
curl http://localhost:9090/metrics
```

### Key Metrics

```
# Throughput
persistq_append_total
persistq_read_total

# Latency
persistq_append_latency_us
persistq_read_latency_us

# Storage
persistq_segments_active
persistq_compaction_total
persistq_bytes_written

# Raft
raft_term
raft_state (0=Follower, 1=Candidate, 2=Leader)
raft_leader_id
```

---

## 🔥 Common Tasks

### Scale Cluster

**Docker:**
```bash
# Add more nodes manually in docker-compose.yml
# Then:
docker-compose up -d
```

**Kubernetes:**
```bash
# Scale to 5 nodes
make k8s-scale REPLICAS=5
# or
kubectl scale statefulset dolda -n dolda --replicas=5
```

### View Logs

**Docker:**
```bash
# All nodes
docker-compose logs -f

# Specific node
docker-compose logs -f dolda-node1
```

**Kubernetes:**
```bash
# Follow logs
kubectl logs -n dolda dolda-0 -f

# Last 100 lines
kubectl logs -n dolda dolda-0 --tail=100

# Previous container (after restart)
kubectl logs -n dolda dolda-0 --previous
```

### Execute Commands

**Docker:**
```bash
docker exec -it dolda-node1 /bin/bash
```

**Kubernetes:**
```bash
kubectl exec -it -n dolda dolda-0 -- /bin/bash
```

### Health Checks

**Docker:**
```bash
curl http://localhost:8081/health
curl http://localhost:8081/health/live
curl http://localhost:8081/health/ready
```

**Kubernetes:**
```bash
kubectl port-forward -n dolda dolda-0 8081:8081
curl http://localhost:8081/health
```

---

## 🛠️ Makefile Commands

### Build & Test
```bash
make build           # Build release binary
make test            # Run tests
make stress-test     # Run stress tests
make bench           # Run benchmarks
```

### Docker
```bash
make docker-build    # Build Docker image
make docker-push     # Push to registry
make docker-run      # Run single container
make docker-stop     # Stop container
```

### Docker Compose
```bash
make compose-up      # Start cluster
make compose-down    # Stop cluster
make compose-logs    # View logs
make compose-ps      # Show status
make compose-clean   # Clean up (incl. volumes)
```

### Kubernetes
```bash
make k8s-deploy      # Deploy to k8s
make k8s-delete      # Delete from k8s
make k8s-status      # Show status
make k8s-pods        # List pods
make k8s-logs        # View logs
make k8s-port-forward # Port forward
```

### Monitoring
```bash
make metrics         # Show metrics
make health          # Check health
make prometheus      # Open Prometheus
make grafana         # Open Grafana
```

### Cleanup
```bash
make clean           # Clean build artifacts
make clean-all       # Clean everything
```

---

## 🔒 Security

### Network Policies

Kubernetes network policies restrict traffic:
- ✅ Allow service traffic (8080)
- ✅ Allow health checks (8081)
- ✅ Allow metrics (9090)
- ✅ Allow Raft between pods (7000)
- ✅ Allow DNS lookups
- ❌ Block all other traffic

### Pod Security

- Non-root user (UID 1000)
- Read-only root filesystem (where possible)
- No privilege escalation
- Dropped capabilities

### Secrets

For production, use Kubernetes Secrets:
```bash
kubectl create secret generic dolda-secrets -n dolda \
  --from-literal=api-key=your-key
```

---

## 🚨 Troubleshooting

### Docker Issues

**Container won't start:**
```bash
docker logs dolda-node1
docker inspect dolda-node1
```

**Network issues:**
```bash
docker network inspect dolda_dolda-network
docker exec dolda-node1 ping dolda-node2
```

### Kubernetes Issues

**Pod stuck in Pending:**
```bash
kubectl describe pod -n dolda dolda-0
kubectl get events -n dolda
```

**Storage issues:**
```bash
kubectl get pvc -n dolda
kubectl describe pvc -n dolda data-dolda-0
```

**Network issues:**
```bash
kubectl exec -n dolda dolda-0 -- nslookup dolda-1.dolda-headless
kubectl exec -n dolda dolda-0 -- ping dolda-1.dolda-headless
```

---

## ✅ Verification Checklist

### Post-Deployment Checks

- [ ] All pods running: `kubectl get pods -n dolda`
- [ ] PVCs bound: `kubectl get pvc -n dolda`
- [ ] Services created: `kubectl get svc -n dolda`
- [ ] Health checks passing: `curl http://localhost:8081/health`
- [ ] Metrics available: `curl http://localhost:9090/metrics`
- [ ] Raft quorum formed: Check logs for leader election
- [ ] Can write data: Test append operation
- [ ] Can read data: Test read operation
- [ ] Monitoring working: Prometheus scraping metrics
- [ ] Failover works: Kill a pod, verify recovery

---

## 📚 Additional Resources

- **Deployment Guide**: `DEPLOYMENT.md` - Complete deployment documentation
- **Cluster Setup**: `CLUSTER_SETUP_GUIDE.md` - Manual cluster setup
- **Stress Testing**: `STRESS_TESTING_GUIDE.md` - Performance testing
- **Main README**: `README.md` - Project overview

---

## 💡 Best Practices

1. ✅ **Always use odd number of nodes** (3, 5, 7)
2. ✅ **Use SSD storage** for data volumes
3. ✅ **Set resource limits** to prevent OOM
4. ✅ **Monitor disk usage** continuously
5. ✅ **Regular backups** of persistent data
6. ✅ **Test failover** before production
7. ✅ **Use PodDisruptionBudget** for HA
8. ✅ **Enable network policies** for security
9. ✅ **Version your images** (avoid :latest in prod)
10. ✅ **Monitor metrics** via Prometheus/Grafana

---

**Status**: ✅ **Production-Ready Docker & Kubernetes Deployment**

Deploy DOLDA with confidence! 🚀

