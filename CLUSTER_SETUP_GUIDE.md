# DOLDA Cluster Setup Guide

## Overview

This guide walks you through setting up a production DOLDA cluster with multiple nodes, Raft consensus, and high availability.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      3-Node DOLDA Cluster                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐│
│   │   Node 1    │      │   Node 2    │      │   Node 3    ││
│   │  (Leader)   │◄────►│  (Follower) │◄────►│  (Follower) ││
│   │             │      │             │      │             ││
│   │ :9001 :8081 │      │ :9002 :8082 │      │ :9003 :8083 ││
│   └─────────────┘      └─────────────┘      └─────────────┘│
│         ▲                      ▲                      ▲      │
│         │                      │                      │      │
│         └──────────────────────┴──────────────────────┘      │
│                    Raft Consensus Network                     │
└─────────────────────────────────────────────────────────────┘
           │                      │                      │
           └──────────────────────┴──────────────────────┘
                          Client Connections
```

## Prerequisites

### Hardware Requirements

**Minimum (Development)**:
```
CPU:     2 cores per node
Memory:  4 GB RAM per node
Storage: 50 GB SSD per node
Network: 1 Gbps
```

**Recommended (Production)**:
```
CPU:     8 cores per node
Memory:  16 GB RAM per node
Storage: 500 GB NVMe SSD per node
Network: 10 Gbps low-latency
```

### Software Requirements

```bash
# Rust toolchain
rustc 1.70+
cargo 1.70+

# System packages
sudo apt-get install -y build-essential libssl-dev pkg-config

# Optional monitoring
prometheus
grafana
```

## Quick Start (3-Node Cluster)

### Step 1: Build DOLDA

```bash
# Clone repository
cd /opt
git clone https://github.com/your-org/dolda.git
cd dolda

# Build release binary
cargo build --release

# Binary will be at: target/release/dolda
```

### Step 2: Create Configuration Files

Create configuration directory:
```bash
mkdir -p /etc/dolda/
```

**Node 1 Configuration** (`/etc/dolda/node1.toml`):
```toml
[node]
id = 1
data_dir = "/var/lib/dolda/node1"

[network]
listen_addr = "0.0.0.0:9001"
advertise_addr = "192.168.1.101:9001"

[raft]
node_id = 1
cluster_nodes = [1, 2, 3]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50

# Peer addresses for Raft communication
[raft.peers]
2 = "192.168.1.102:9002"
3 = "192.168.1.103:9003"

[storage]
segment_size_mb = 10
index_enabled = true
flush_interval_secs = 5

[compaction]
enabled = true
min_utilization = 0.6
min_age_secs = 600
check_interval_secs = 120

[health]
listen_addr = "0.0.0.0:8081"
check_interval_secs = 5

[observability]
metrics_addr = "0.0.0.0:9091"
log_level = "info"
```

**Node 2 Configuration** (`/etc/dolda/node2.toml`):
```toml
[node]
id = 2
data_dir = "/var/lib/dolda/node2"

[network]
listen_addr = "0.0.0.0:9002"
advertise_addr = "192.168.1.102:9002"

[raft]
node_id = 2
cluster_nodes = [1, 2, 3]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50

[raft.peers]
1 = "192.168.1.101:9001"
3 = "192.168.1.103:9003"

[storage]
segment_size_mb = 10
index_enabled = true
flush_interval_secs = 5

[compaction]
enabled = true
min_utilization = 0.6
min_age_secs = 600
check_interval_secs = 120

[health]
listen_addr = "0.0.0.0:8082"
check_interval_secs = 5

[observability]
metrics_addr = "0.0.0.0:9092"
log_level = "info"
```

**Node 3 Configuration** (`/etc/dolda/node3.toml`):
```toml
[node]
id = 3
data_dir = "/var/lib/dolda/node3"

[network]
listen_addr = "0.0.0.0:9003"
advertise_addr = "192.168.1.103:9003"

[raft]
node_id = 3
cluster_nodes = [1, 2, 3]
election_timeout_min_ms = 150
election_timeout_max_ms = 300
heartbeat_interval_ms = 50

[raft.peers]
1 = "192.168.1.101:9001"
2 = "192.168.1.102:9002"

[storage]
segment_size_mb = 10
index_enabled = true
flush_interval_secs = 5

[compaction]
enabled = true
min_utilization = 0.6
min_age_secs = 600
check_interval_secs = 120

[health]
listen_addr = "0.0.0.0:8083"
check_interval_secs = 5

[observability]
metrics_addr = "0.0.0.0:9093"
log_level = "info"
```

### Step 3: Create Data Directories

On each node:
```bash
sudo mkdir -p /var/lib/dolda/node{1,2,3}
sudo chown -R dolda:dolda /var/lib/dolda
```

### Step 4: Create Systemd Service Files

**Node 1** (`/etc/systemd/system/dolda-node1.service`):
```ini
[Unit]
Description=DOLDA Node 1
After=network.target

[Service]
Type=simple
User=dolda
Group=dolda
WorkingDirectory=/opt/dolda
ExecStart=/opt/dolda/target/release/dolda --config /etc/dolda/node1.toml
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

[Install]
WantedBy=multi-user.target
```

Create similar files for Node 2 and Node 3, adjusting the paths and config files.

### Step 5: Start the Cluster

```bash
# On each node, enable and start the service
sudo systemctl daemon-reload
sudo systemctl enable dolda-node1  # or node2, node3
sudo systemctl start dolda-node1

# Check status
sudo systemctl status dolda-node1

# View logs
sudo journalctl -u dolda-node1 -f
```

### Step 6: Verify Cluster Health

Check each node's health:
```bash
# Node 1
curl http://192.168.1.101:8081/health/detail | jq

# Node 2
curl http://192.168.1.102:8082/health/detail | jq

# Node 3
curl http://192.168.1.103:8083/health/detail | jq
```

Expected output:
```json
{
  "status": "healthy",
  "uptime_seconds": 120,
  "components": [
    {
      "name": "storage",
      "status": "healthy",
      "message": "All systems operational"
    },
    {
      "name": "raft",
      "status": "healthy",
      "message": "Leader elected, term 1"
    },
    {
      "name": "network",
      "status": "healthy",
      "message": "Connected to 2 peers"
    }
  ],
  "version": "0.1.0"
}
```

## Advanced Configuration

### Network Topology

**Single Datacenter**:
```
All nodes in same datacenter with low latency (<1ms)
- Recommended for: Maximum performance
- Trade-off: No geographic redundancy
```

**Multi-Datacenter**:
```
Nodes distributed across 2-3 datacenters
- Recommended for: Geographic redundancy
- Trade-off: Higher latency, requires tuning
- Config: Increase election_timeout to 500-1000ms
```

### Cluster Sizing

**3-Node Cluster** (Recommended):
```
- Tolerates: 1 node failure
- Quorum: 2 nodes
- Use case: Standard production deployments
```

**5-Node Cluster**:
```
- Tolerates: 2 node failures
- Quorum: 3 nodes
- Use case: Mission-critical, high-availability
```

**7-Node Cluster**:
```
- Tolerates: 3 node failures
- Quorum: 4 nodes
- Use case: Maximum availability requirements
- Note: Diminishing returns beyond 7 nodes
```

### Firewall Rules

Open the following ports:

```bash
# Raft consensus (node-to-node)
sudo ufw allow 9001:9003/tcp

# Health check endpoints
sudo ufw allow 8081:8083/tcp

# Prometheus metrics
sudo ufw allow 9091:9093/tcp

# Optional: Client connections
sudo ufw allow 10000:10002/tcp
```

## Production Deployment

### Using Docker Compose

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  dolda-node1:
    build: .
    container_name: dolda-node1
    hostname: dolda-node1
    ports:
      - "9001:9001"
      - "8081:8081"
      - "9091:9091"
    volumes:
      - ./data/node1:/var/lib/dolda
      - ./config/node1.toml:/etc/dolda/config.toml
    environment:
      - RUST_LOG=info
    networks:
      - dolda-network
    restart: unless-stopped

  dolda-node2:
    build: .
    container_name: dolda-node2
    hostname: dolda-node2
    ports:
      - "9002:9002"
      - "8082:8082"
      - "9092:9092"
    volumes:
      - ./data/node2:/var/lib/dolda
      - ./config/node2.toml:/etc/dolda/config.toml
    environment:
      - RUST_LOG=info
    networks:
      - dolda-network
    restart: unless-stopped

  dolda-node3:
    build: .
    container_name: dolda-node3
    hostname: dolda-node3
    ports:
      - "9003:9003"
      - "8083:8083"
      - "9093:9093"
    volumes:
      - ./data/node3:/var/lib/dolda
      - ./config/node3.toml:/etc/dolda/config.toml
    environment:
      - RUST_LOG=info
    networks:
      - dolda-network
    restart: unless-stopped

networks:
  dolda-network:
    driver: bridge
```

Start the cluster:
```bash
docker-compose up -d
```

### Using Kubernetes

Create `dolda-statefulset.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: dolda-cluster
  labels:
    app: dolda
spec:
  clusterIP: None
  selector:
    app: dolda
  ports:
  - name: raft
    port: 9000
  - name: health
    port: 8080
  - name: metrics
    port: 9090
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: dolda
spec:
  serviceName: dolda-cluster
  replicas: 3
  selector:
    matchLabels:
      app: dolda
  template:
    metadata:
      labels:
        app: dolda
    spec:
      containers:
      - name: dolda
        image: dolda:latest
        ports:
        - containerPort: 9000
          name: raft
        - containerPort: 8080
          name: health
        - containerPort: 9090
          name: metrics
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: RUST_LOG
          value: "info"
        volumeMounts:
        - name: data
          mountPath: /var/lib/dolda
        - name: config
          mountPath: /etc/dolda
        livenessProbe:
          httpGet:
            path: /health
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 5
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"
          limits:
            cpu: "4"
            memory: "8Gi"
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 100Gi
      storageClassName: fast-ssd
```

Deploy:
```bash
kubectl apply -f dolda-statefulset.yaml
kubectl get pods -l app=dolda
kubectl logs -f dolda-0
```

## Monitoring Setup

### Prometheus Configuration

Create `prometheus.yml`:

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'dolda-cluster'
    static_configs:
      - targets:
        - '192.168.1.101:9091'
        - '192.168.1.102:9092'
        - '192.168.1.103:9093'
        labels:
          cluster: 'production'

  - job_name: 'dolda-health'
    metrics_path: '/health/detail'
    static_configs:
      - targets:
        - '192.168.1.101:8081'
        - '192.168.1.102:8082'
        - '192.168.1.103:8083'
```

### Grafana Dashboard

Import dashboard JSON (create `dolda-dashboard.json`):

Key panels to monitor:
- Raft leader status
- Election count
- Write throughput
- Read latency
- Compaction activity
- Disk usage
- Network traffic

## Operational Procedures

### Adding a New Node

1. **Prepare the new node**:
```bash
# Install DOLDA on new node
sudo cp /opt/dolda/target/release/dolda /usr/local/bin/
```

2. **Create configuration**:
```toml
[node]
id = 4
data_dir = "/var/lib/dolda/node4"

[raft]
node_id = 4
cluster_nodes = [1, 2, 3, 4]  # Add new node
...
```

3. **Update existing nodes** (rolling update):
```bash
# Update config on each existing node to include node 4
# Restart nodes one at a time
sudo systemctl restart dolda-node1
# Wait for node to rejoin cluster
# Repeat for other nodes
```

4. **Start new node**:
```bash
sudo systemctl start dolda-node4
```

### Removing a Node

1. **Stop the node**:
```bash
sudo systemctl stop dolda-node3
```

2. **Update configurations** on remaining nodes:
```toml
[raft]
cluster_nodes = [1, 2]  # Remove node 3
```

3. **Restart remaining nodes**:
```bash
# Rolling restart
sudo systemctl restart dolda-node1
# Wait for stability
sudo systemctl restart dolda-node2
```

### Handling Node Failures

**Temporary Failure** (node will recover):
```bash
# Cluster continues operating with quorum
# Failed node will catch up when restarted
sudo systemctl start dolda-node2
```

**Permanent Failure** (data lost):
```bash
# 1. Remove failed node from cluster
# 2. Add replacement node with new ID
# 3. Data will be replicated from leader
```

### Backup and Recovery

**Backup Strategy**:
```bash
#!/bin/bash
# backup-dolda.sh

NODE_ID=1
DATA_DIR="/var/lib/dolda/node${NODE_ID}"
BACKUP_DIR="/backup/dolda/$(date +%Y%m%d)"

# Stop writes (optional, for consistent backup)
# curl -X POST http://localhost:8081/admin/pause

# Create backup
mkdir -p $BACKUP_DIR
rsync -av --exclude='*.tmp' $DATA_DIR/ $BACKUP_DIR/

# Resume writes
# curl -X POST http://localhost:8081/admin/resume

echo "Backup completed: $BACKUP_DIR"
```

**Recovery**:
```bash
#!/bin/bash
# restore-dolda.sh

NODE_ID=1
DATA_DIR="/var/lib/dolda/node${NODE_ID}"
BACKUP_DIR="/backup/dolda/20251110"

# Stop node
sudo systemctl stop dolda-node${NODE_ID}

# Restore data
rm -rf $DATA_DIR/*
rsync -av $BACKUP_DIR/ $DATA_DIR/

# Start node
sudo systemctl start dolda-node${NODE_ID}
```

## Troubleshooting

### Cluster Won't Form

**Symptom**: Nodes keep timing out, no leader elected

**Solution**:
```bash
# 1. Check network connectivity
ping 192.168.1.101
telnet 192.168.1.101 9001

# 2. Check firewall rules
sudo ufw status

# 3. Verify configuration
grep -A 5 "\[raft\]" /etc/dolda/node1.toml

# 4. Check logs
sudo journalctl -u dolda-node1 | grep "election"
```

### Split Brain Scenario

**Symptom**: Two leaders elected (should never happen with proper quorum)

**Solution**:
```bash
# This indicates a serious network partition

# 1. Identify the actual leader
curl http://node1:8081/health/detail | jq '.components[] | select(.name=="raft")'
curl http://node2:8082/health/detail | jq '.components[] | select(.name=="raft")'

# 2. Check network connectivity between all nodes
# 3. Verify no network equipment is causing partition
# 4. If unresolvable, restart minority partition nodes
```

### High Write Latency

**Solution**:
```bash
# 1. Check disk I/O
iostat -x 1

# 2. Check if compaction is running
curl http://node1:9091/metrics | grep compaction

# 3. Adjust compaction policy
# Edit /etc/dolda/node1.toml
[compaction]
check_interval_secs = 300  # Reduce frequency

# 4. Increase segment size
[storage]
segment_size_mb = 50  # Larger segments
```

### Node Repeatedly Failing Health Checks

**Solution**:
```bash
# 1. Check resource usage
top
df -h /var/lib/dolda

# 2. Check for disk space
du -sh /var/lib/dolda/*

# 3. Trigger manual compaction if needed
# (Implementation would provide admin endpoint)
curl -X POST http://node1:8081/admin/compact

# 4. Increase resource limits
# Edit /etc/systemd/system/dolda-node1.service
[Service]
LimitNOFILE=131072
```

## Performance Tuning

### Network Optimization

```bash
# Increase TCP buffer sizes
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w net.ipv4.tcp_rmem='4096 87380 134217728'
sudo sysctl -w net.ipv4.tcp_wmem='4096 65536 134217728'

# Enable TCP fast open
sudo sysctl -w net.ipv4.tcp_fastopen=3
```

### Disk Optimization

```bash
# Use deadline scheduler for SSDs
echo deadline | sudo tee /sys/block/sda/queue/scheduler

# Disable disk barriers (if using battery-backed RAID)
# Add to /etc/fstab: nobarrier option

# Mount with noatime
mount -o remount,noatime /var/lib/dolda
```

### Memory Tuning

```toml
# In config.toml
[storage]
cache_size_mb = 1024  # Adjust based on available RAM
```

## Security

### TLS Configuration

```toml
[network]
tls_enabled = true
tls_cert = "/etc/dolda/certs/server.crt"
tls_key = "/etc/dolda/certs/server.key"
tls_ca = "/etc/dolda/certs/ca.crt"
```

Generate certificates:
```bash
# Create CA
openssl req -new -x509 -days 3650 -keyout ca.key -out ca.crt

# Create node certificates
for node in 1 2 3; do
  openssl req -new -keyout node${node}.key -out node${node}.csr
  openssl x509 -req -in node${node}.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out node${node}.crt -days 365
done
```

### Authentication

```toml
[security]
auth_enabled = true
auth_token = "your-secure-token-here"
admin_users = ["admin@example.com"]
```

## Summary

Your DOLDA cluster is now ready for production! Key points:

- ✅ Minimum 3 nodes for high availability
- ✅ Raft consensus for coordination
- ✅ Health monitoring on all nodes
- ✅ Prometheus metrics collection
- ✅ Automated compaction
- ✅ Proper backup procedures

For production use, ensure:
1. Regular backups
2. Monitoring alerts configured
3. Disaster recovery plan tested
4. Security hardening applied
5. Performance baselines established

**Next Steps**: Review the monitoring dashboards and establish operational runbooks!

