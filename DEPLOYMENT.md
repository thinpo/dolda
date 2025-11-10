# DOLDA Deployment Guide

This guide covers deploying DOLDA using Docker and Kubernetes.

---

## 🐳 Docker Deployment

### Quick Start with Docker Compose

```bash
# Build and start 3-node cluster
docker-compose up -d

# Check cluster status
docker-compose ps

# View logs
docker-compose logs -f

# Scale to 5 nodes
docker-compose up -d --scale dolda-node=5

# Stop cluster
docker-compose down

# Clean up volumes
docker-compose down -v
```

### Build Docker Image

```bash
# Build image
docker build -t dolda:latest .

# Build with specific tag
docker build -t your-registry/dolda:v0.1.0 .

# Push to registry
docker push your-registry/dolda:v0.1.0
```

### Run Single Node

```bash
docker run -d \
  --name dolda \
  -p 8080:8080 \
  -p 8081:8081 \
  -p 9090:9090 \
  -v dolda-data:/data \
  -v dolda-logs:/logs \
  -e DOLDA_NODE_ID=1 \
  -e DOLDA_NODE_NAME=node1 \
  -e RUST_LOG=info \
  dolda:latest
```

### Monitoring Stack

The Docker Compose setup includes:
- **Prometheus** (port 9093): Metrics collection
- **Grafana** (port 3000): Visualization dashboards

Access Grafana:
```
URL: http://localhost:3000
Username: admin
Password: admin
```

---

## ☸️ Kubernetes Deployment

### Prerequisites

```bash
# Verify kubectl access
kubectl version

# Verify cluster connection
kubectl cluster-info

# Check storage classes
kubectl get storageclass
```

### Deploy DOLDA Cluster

#### Option 1: Using kubectl

```bash
# Apply all manifests
kubectl apply -f k8s/

# Or apply individually
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/statefulset.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/poddisruptionbudget.yaml
kubectl apply -f k8s/networkpolicy.yaml
```

#### Option 2: Using Kustomize

```bash
# Deploy using kustomize
kubectl apply -k k8s/

# With custom overlay
kubectl apply -k k8s/overlays/production/
```

#### Option 3: Using Helm (if you create a chart)

```bash
# Install
helm install dolda ./helm/dolda -n dolda --create-namespace

# Upgrade
helm upgrade dolda ./helm/dolda -n dolda

# Uninstall
helm uninstall dolda -n dolda
```

### Verify Deployment

```bash
# Check namespace
kubectl get namespace dolda

# Check pods
kubectl get pods -n dolda -w

# Check services
kubectl get svc -n dolda

# Check persistent volumes
kubectl get pvc -n dolda

# Check logs
kubectl logs -n dolda dolda-0 -f

# Check all resources
kubectl get all -n dolda
```

### Health Checks

```bash
# Port-forward to access health endpoint
kubectl port-forward -n dolda dolda-0 8081:8081

# Check health (in another terminal)
curl http://localhost:8081/health
curl http://localhost:8081/health/live
curl http://localhost:8081/health/ready

# Check metrics
kubectl port-forward -n dolda dolda-0 9090:9090
curl http://localhost:9090/metrics
```

### Access the Service

```bash
# Get LoadBalancer IP
kubectl get svc -n dolda dolda-service

# Port-forward for local access
kubectl port-forward -n dolda svc/dolda-service 8080:8080

# Test connection
curl http://localhost:8080/
```

---

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DOLDA_NODE_ID` | - | Unique node identifier |
| `DOLDA_NODE_NAME` | - | Node name |
| `DOLDA_LISTEN_ADDR` | `0.0.0.0:8080` | Service listen address |
| `DOLDA_RAFT_ADDR` | `0.0.0.0:7000` | Raft consensus address |
| `DOLDA_HEALTH_ADDR` | `0.0.0.0:8081` | Health check address |
| `DOLDA_METRICS_ADDR` | `0.0.0.0:9090` | Metrics endpoint address |
| `DOLDA_DATA_DIR` | `/data` | Data storage directory |
| `DOLDA_LOG_LEVEL` | `info` | Log level (debug/info/warn/error) |
| `DOLDA_PEERS` | - | Comma-separated peer addresses |
| `RUST_LOG` | `info` | Rust logging level |

### Storage Configuration

**Docker Volumes:**
- `/data`: Persistent data storage
- `/logs`: Application logs

**Kubernetes PersistentVolumeClaims:**
- `data`: 100Gi (SSD recommended)
- `logs`: 10Gi (Standard storage)

Adjust in `k8s/statefulset.yaml`:
```yaml
volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      storageClassName: fast-ssd  # Change to your storage class
      resources:
        requests:
          storage: 100Gi  # Adjust size
```

### Resource Limits

Default Kubernetes resources:
```yaml
resources:
  requests:
    cpu: 500m
    memory: 1Gi
  limits:
    cpu: 2000m
    memory: 4Gi
```

Adjust based on workload:
- **Small**: 500m CPU, 1Gi memory
- **Medium**: 1 CPU, 2Gi memory
- **Large**: 2+ CPU, 4+ Gi memory

---

## 📊 Monitoring

### Prometheus Metrics

DOLDA exposes Prometheus metrics on port 9090:

```bash
# View available metrics
curl http://localhost:9090/metrics | grep persistq
```

Key metrics:
- `persistq_append_total` - Total append operations
- `persistq_append_latency_us` - Append latency
- `persistq_read_total` - Total read operations
- `persistq_segments_active` - Active segments
- `persistq_compaction_total` - Compaction operations

### Kubernetes ServiceMonitor

If using Prometheus Operator:

```bash
kubectl apply -f k8s/servicemonitor.yaml
```

### Grafana Dashboards

Import dashboards from `monitoring/grafana/dashboards/` (create these based on your metrics).

---

## 🔄 Scaling

### Docker Compose

```bash
# Scale to 5 nodes
docker-compose up -d --scale dolda-node=5

# Scale down to 3 nodes
docker-compose up -d --scale dolda-node=3
```

### Kubernetes

```bash
# Scale StatefulSet
kubectl scale statefulset dolda -n dolda --replicas=5

# Or edit the StatefulSet
kubectl edit statefulset dolda -n dolda
```

**Important**: Always maintain an odd number of nodes (3, 5, 7) for Raft quorum.

---

## 🔒 Security

### Network Policies

Network policies are included to restrict traffic:
- Allow service traffic (8080)
- Allow health checks (8081)
- Allow metrics scraping (9090)
- Allow Raft between pods (7000)

```bash
kubectl apply -f k8s/networkpolicy.yaml
```

### Pod Security

Pods run as non-root user (UID 1000):
```yaml
securityContext:
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
```

### Secrets Management

For sensitive configuration, use Kubernetes Secrets:

```bash
# Create secret
kubectl create secret generic dolda-secrets -n dolda \
  --from-literal=api-key=your-api-key

# Reference in pod
env:
  - name: API_KEY
    valueFrom:
      secretKeyRef:
        name: dolda-secrets
        key: api-key
```

---

## 🚨 High Availability

### Pod Disruption Budget

Ensures at least 2 nodes are always running:

```bash
kubectl apply -f k8s/poddisruptionbudget.yaml
```

### Anti-Affinity

Pods are spread across different nodes:
```yaml
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
```

### Backup Strategy

**Automated backups** (example):
```bash
# Backup PVC data
kubectl exec -n dolda dolda-0 -- tar czf /backup/dolda-backup.tar.gz /data

# Copy backup
kubectl cp dolda/dolda-0:/backup/dolda-backup.tar.gz ./dolda-backup.tar.gz
```

---

## 🐛 Troubleshooting

### Check Pod Status

```bash
# Get pod status
kubectl get pods -n dolda

# Describe pod
kubectl describe pod -n dolda dolda-0

# View logs
kubectl logs -n dolda dolda-0 --tail=100
kubectl logs -n dolda dolda-0 --previous  # Previous container
```

### Common Issues

**Pod stuck in Pending:**
```bash
# Check PVC status
kubectl get pvc -n dolda

# Check events
kubectl get events -n dolda --sort-by='.lastTimestamp'
```

**Raft not forming quorum:**
```bash
# Check peer connectivity
kubectl exec -n dolda dolda-0 -- ping dolda-1.dolda-headless

# Check Raft logs
kubectl logs -n dolda dolda-0 | grep -i raft
```

**Performance issues:**
```bash
# Check resource usage
kubectl top pods -n dolda

# Check metrics
kubectl port-forward -n dolda dolda-0 9090:9090
curl http://localhost:9090/metrics | grep persistq
```

### Docker Troubleshooting

```bash
# View container logs
docker logs dolda-node1 -f

# Execute shell in container
docker exec -it dolda-node1 /bin/bash

# Check container stats
docker stats dolda-node1

# Inspect container
docker inspect dolda-node1
```

---

## 🧪 Testing

### Stress Test

```bash
# Port-forward service
kubectl port-forward -n dolda svc/dolda-service 8080:8080

# Run stress test (from another terminal)
cargo test --release --test storage_stress -- --nocapture
```

### Integration Test

```bash
# Test write
curl -X POST http://localhost:8080/append \
  -H "Content-Type: application/json" \
  -d '{"data": "test message"}'

# Test read
curl http://localhost:8080/read?offset=0

# Test health
curl http://localhost:8081/health
```

---

## 📦 Upgrading

### Rolling Update (Kubernetes)

```bash
# Update image
kubectl set image statefulset/dolda dolda=dolda:v0.2.0 -n dolda

# Watch rollout
kubectl rollout status statefulset/dolda -n dolda

# Rollback if needed
kubectl rollout undo statefulset/dolda -n dolda
```

### Docker Compose

```bash
# Pull new image
docker-compose pull

# Recreate containers
docker-compose up -d --force-recreate
```

---

## 🧹 Cleanup

### Docker

```bash
# Stop and remove containers
docker-compose down

# Remove volumes
docker-compose down -v

# Remove images
docker rmi dolda:latest
```

### Kubernetes

```bash
# Delete all resources
kubectl delete -f k8s/

# Or delete namespace (deletes everything)
kubectl delete namespace dolda

# Delete PVCs (data will be lost!)
kubectl delete pvc -n dolda --all
```

---

## 📚 Additional Resources

- **Main README**: `README.md`
- **Cluster Setup**: `CLUSTER_SETUP_GUIDE.md`
- **Stress Testing**: `STRESS_TESTING_GUIDE.md`
- **Production Status**: `PRODUCTION_STATUS.md`

---

## 💡 Best Practices

1. **Always use odd number of nodes** (3, 5, 7) for Raft quorum
2. **Monitor disk usage** - PVCs can fill up quickly
3. **Set resource limits** to prevent OOM kills
4. **Use SSD storage** for data volumes (fast-ssd storage class)
5. **Regular backups** of PVC data
6. **Test failover** scenarios before production
7. **Monitor metrics** continuously via Prometheus/Grafana
8. **Use Pod Disruption Budgets** to maintain availability
9. **Enable network policies** for security
10. **Version your images** - avoid using `:latest` in production

---

**Status**: ✅ Production-Ready Deployment Configurations

Ready to deploy DOLDA to Docker or Kubernetes! 🚀

