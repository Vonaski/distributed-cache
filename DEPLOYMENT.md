# Deployment Guide - 2 VPS Servers

This guide covers deploying the distributed cache system on 2 VPS servers with 4 vCPU and 8GB RAM each.

## Architecture Overview

### Server 1 (VPS-1): 4 vCPU, 8GB RAM
```
Component          | CPU Limit | Memory Limit | Memory Actual
-------------------|-----------|--------------|---------------
Nginx              | 0.5       | 256M         | ~100M
Proxy-1            | 1.0       | 768M         | ~640M (512M heap + 128M direct)
Proxy-2            | 1.0       | 768M         | ~640M (512M heap + 128M direct)
Cache-Master-1     | 2.0       | 2560M        | ~2512M (2GB heap + 512M direct)
Cache-Master-2     | 2.0       | 2560M        | ~2512M (2GB heap + 512M direct)
Cache-Replica-3    | 1.5       | 2048M        | ~1920M (1.5GB heap + 384M direct)
Prometheus         | 0.5       | 768M         | ~512M
-------------------|-----------|--------------|---------------
TOTAL              | 8.5       | 9728M        | ~7.8GB
```

### Server 2 (VPS-2): 4 vCPU, 8GB RAM
```
Component          | CPU Limit | Memory Limit | Memory Actual
-------------------|-----------|--------------|---------------
Proxy-3            | 1.0       | 768M         | ~640M (512M heap + 128M direct)
Cache-Master-3     | 2.0       | 2560M        | ~2512M (2GB heap + 512M direct)
Cache-Replica-1    | 1.5       | 2048M        | ~1920M (1.5GB heap + 384M direct)
Cache-Replica-2    | 1.5       | 2048M        | ~1920M (1.5GB heap + 384M direct)
Grafana            | 0.5       | 512M         | ~256M
Loki               | 0.5       | 768M         | ~512M
-------------------|-----------|--------------|---------------
TOTAL              | 7.0       | 8704M        | ~7.7GB
```

## JVM Configuration Explained

All Java applications use optimized JVM settings for low-latency caching:

### ZGC (Z Garbage Collector)
```bash
-XX:+UseZGC                    # Enable ZGC
-XX:+ZGenerational             # Use generational ZGC (Java 21+)
```

**Why ZGC?**
- Ultra-low pause times (<1ms)
- Works well with large heaps (our masters have 2GB)
- Better than G1GC for latency-sensitive applications
- Generational mode improves throughput

### Heap Settings
```bash
-Xmx2g -Xms2g                  # Masters: 2GB heap (min=max for predictability)
-Xmx1536m -Xms1536m            # Replicas: 1.5GB heap
-Xmx512m -Xms512m              # Proxies: 512MB heap
```

**Why equal min/max?**
- JVM allocates full heap at startup
- No runtime heap resizing (eliminates pauses)
- `-XX:+AlwaysPreTouch` pre-faults all pages (faster first access)

### Direct Memory (Netty Buffers)
```bash
-XX:MaxDirectMemorySize=512m   # Masters: 512MB for direct buffers
-XX:MaxDirectMemorySize=384m   # Replicas: 384MB
-XX:MaxDirectMemorySize=128m   # Proxies: 128MB
```

**Why direct memory?**
- Netty uses off-heap buffers for zero-copy I/O
- Cached in `sun.misc.Unsafe` (native memory)
- Faster network operations (no copying between heap/kernel)
- Less GC pressure

### Netty Configuration
```bash
-Dio.netty.allocator.type=pooled         # Use pooled byte buffer allocator
-Dio.netty.maxDirectMemory=0             # Let JVM manage direct memory limit
-Dio.netty.leakDetection.level=simple    # Detect buffer leaks (production-safe)
```

**What this does:**
- **Pooled allocator**: Reuses ByteBuf objects (reduces allocations)
- **Direct buffers**: Netty allocates buffers outside JVM heap (faster I/O)
- **Leak detection**: Warns if buffers are not released (memory leak prevention)

### Other Optimizations
```bash
-XX:+UseStringDeduplication    # Deduplicate identical strings (saves memory)
-XX:+AlwaysPreTouch            # Touch all memory pages at startup (predictable performance)
-XX:+UnlockExperimentalVMOptions  # Required for some ZGC options
```

## Prerequisites

### On Both VPS Servers:
1. Docker 20.10+
2. Docker Compose 2.0+
3. At least 8GB RAM available
4. At least 20GB disk space

### Network Requirements:
- Servers can communicate with each other (same VPC or VPN)
- Open ports between servers (7000-7105, 8080-8086)
- External access to port 80 (Nginx) on Server 1

## Step-by-Step Deployment

### 1. Prepare Servers

On both servers:
```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Add current user to docker group
sudo usermod -aG docker $USER
newgrp docker

# Verify installation
docker --version
docker-compose --version
```

### 2. Setup Docker Network Between Servers

If servers are in different networks, set up overlay network or VPN.

**Option A: Docker Swarm (Recommended)**
```bash
# On Server 1 (manager):
docker swarm init --advertise-addr <SERVER_1_IP>
# This outputs a join token, copy it

# On Server 2 (worker):
docker swarm join --token <TOKEN> <SERVER_1_IP>:2377
```

**Option B: Manual Network Configuration**
Update `docker-compose.yml` to use host IPs instead of container names for cross-server communication.

### 3. Build and Push Docker Images

On your development machine:

```bash
# Login to your Docker registry
docker login

# Set registry in environment
export REGISTRY=docker.io/your-username
export TAG=v1.0.0

# Build images
docker-compose build

# Tag images
docker tag distributed-cache-proxy:latest ${REGISTRY}/cache-proxy:${TAG}
docker tag distributed-cache-cache-node:latest ${REGISTRY}/cache-node:${TAG}

# Push to registry
docker push ${REGISTRY}/cache-proxy:${TAG}
docker push ${REGISTRY}/cache-node:${TAG}
```

### 4. Deploy on Server 1

```bash
# Clone repository
git clone <your-repo-url>
cd distributed-cache

# Create production environment file
cp .env.example .env.prod

# Edit .env.prod
nano .env.prod
```

**Update .env.prod with your values:**
```bash
REGISTRY=docker.io/your-username
TAG=v1.0.0

# Update cache nodes to use Server 2 IP for master-3
CACHE_NODES=master-1:localhost:7000:7100,master-2:localhost:7001:7101,master-3:<SERVER_2_IP>:7002:7102
```

**Create docker-compose.server1.yml:**
```yaml
# Include only services for Server 1
version: '3.8'
services:
  nginx:
  proxy-1:
  proxy-2:
  cache-master-1:
  cache-master-2:
  cache-replica-3:
  prometheus:
```

**Start services:**
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.server1.yml up -d
```

### 5. Deploy on Server 2

```bash
# Clone repository
git clone <your-repo-url>
cd distributed-cache

# Create production environment file
cp .env.example .env.prod
nano .env.prod
```

**Update .env.prod:**
```bash
REGISTRY=docker.io/your-username
TAG=v1.0.0

# Update replica masters to point to Server 1
REPLICA_1_MASTER_HOST=<SERVER_1_IP>
REPLICA_2_MASTER_HOST=<SERVER_1_IP>
REPLICA_3_MASTER_HOST=<SERVER_1_IP>
```

**Create docker-compose.server2.yml:**
```yaml
# Include only services for Server 2
version: '3.8'
services:
  proxy-3:
  cache-master-3:
  cache-replica-1:
  cache-replica-2:
  grafana:
  loki:
```

**Start services:**
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.server2.yml up -d
```

### 6. Verify Deployment

**On Server 1:**
```bash
# Check running containers
docker ps

# Check logs
docker-compose logs -f cache-master-1

# Test Nginx
curl http://localhost/health

# Test cache operation
curl -X PUT http://localhost/cache/test -H "Content-Type: application/json" -d '{"value":"hello"}'
curl http://localhost/cache/test
```

**On Server 2:**
```bash
# Check running containers
docker ps

# Check replication status
docker-compose logs -f cache-replica-1
```

**Access monitoring:**
- Prometheus: `http://<SERVER_1_IP>:9090`
- Grafana: `http://<SERVER_2_IP>:3000` (admin/admin)

## Monitoring

### Key Metrics to Watch

1. **Memory Usage**
   - JVM heap should stay around 70-80% of Xmx
   - Direct memory usage (Netty buffers)
   - Container memory limit adherence

2. **GC Pauses (should be <1ms with ZGC)**
   - Check in GC logs
   - Monitor in Prometheus: `jvm_gc_pause_seconds`

3. **Replication Lag**
   - Should be <100ms typically
   - Monitor: `replication_lag_seconds`

4. **Cache Hit Rate**
   - Target: >80%
   - Monitor: `cache_hits_total / (cache_hits_total + cache_misses_total)`

### Enable JVM Metrics

Add to JAVA_OPTS if needed:
```bash
-XX:+PrintGCDetails -XX:+PrintGCDateStamps -Xlog:gc*:file=/logs/gc.log
```

## Troubleshooting

### High Memory Usage
```bash
# Check JVM heap usage
docker exec cache-master-1 jcmd 1 GC.heap_info

# Check Netty direct memory
docker exec cache-master-1 jcmd 1 VM.native_memory summary
```

### GC Pauses Too Long
- Reduce heap size if > 4GB
- Check if ZGC is actually enabled: `docker exec cache-master-1 jcmd 1 VM.flags | grep ZGC`
- Consider switching to Generational ZGC if not using it

### Netty Buffer Leaks
```bash
# Check logs for leak warnings
docker-compose logs cache-master-1 | grep "LEAK"

# Increase leak detection if needed
-Dio.netty.leakDetection.level=paranoid  # Very slow, use only for debugging
```

### Out of Memory
1. Check actual memory usage: `docker stats`
2. Reduce heap if OOM is in container limit
3. Increase container memory limit if heap is appropriate

### Cross-Server Communication Issues
```bash
# Test connectivity from Server 1 to Server 2
docker exec cache-master-1 ping <SERVER_2_IP>
docker exec cache-master-1 telnet <SERVER_2_IP> 7002

# Check firewall rules
sudo ufw status
sudo iptables -L
```

## Scaling Considerations

### Vertical Scaling (More Resources)
- Increase heap: `-Xmx4g` (masters only, if more RAM available)
- Increase direct memory proportionally
- Update `docker-compose.prod.yml` limits

### Horizontal Scaling
- Add more proxy instances (cheap)
- Add more shards (requires rehashing)
- Add more replicas per master

## Backup and Recovery

### Backup Data Volumes
```bash
# On each server
docker run --rm -v cache-master-1-data:/data -v $(pwd):/backup alpine tar czf /backup/master-1-data.tar.gz /data
```

### Restore Data
```bash
docker run --rm -v cache-master-1-data:/data -v $(pwd):/backup alpine sh -c "cd /data && tar xzf /backup/master-1-data.tar.gz --strip 1"
```

## Performance Tuning Tips

1. **Monitor ZGC performance**: Should see <1ms pauses
2. **Netty buffer pool**: Monitor with `-Dio.netty.leakDetection.level=simple`
3. **Network latency**: Keep servers in same datacenter/region
4. **Disk I/O**: Use SSD for Docker volumes
5. **CPU pinning**: Consider Docker CPU sets for cache nodes

## Security Checklist

- [ ] Change Grafana admin password
- [ ] Setup firewall rules (only allow required ports)
- [ ] Use private network for inter-server communication
- [ ] Enable Docker content trust
- [ ] Regular security updates
- [ ] Monitor for suspicious activity

## Maintenance

### Update Deployment
```bash
# Pull new images
docker-compose pull

# Restart with zero downtime (one server at a time)
docker-compose up -d --no-deps --build cache-master-1
```

### View Logs
```bash
# All logs
docker-compose logs -f

# Specific service
docker-compose logs -f cache-master-1

# Last 100 lines
docker-compose logs --tail=100 cache-master-1
```

### Restart Services
```bash
# Restart specific service
docker-compose restart cache-master-1

# Restart all on server
docker-compose restart
```
