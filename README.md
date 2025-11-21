# Distributed Cache System

A high-performance distributed caching system with consistent hashing, master-replica replication, and comprehensive monitoring. Optimized for low-latency operations with ZGC and Netty direct buffers.

## Architecture

```
┌──────────┐
│  Nginx   │  Load Balancer
└────┬─────┘
     │
     ├────────┬──────────┐
     │        │          │       
┌────▼────┐ ┌─▼───────┐ ┌▼────────┐
│ Proxy-1 │ │ Proxy-2 │ │ Proxy-3 │  Consistent Hash Ring
└────┬────┘ └─┬───────┘ └┬────────┘
     │        │          │
     └────────┼──────────┘
              │
     ┌────────┼─────────┐
     │        │         │     
┌────▼────┐ ┌─▼──────┐ ┌▼───────┐
│ Master1 │ │ Master2│ │ Master3│  Cache Shards
└────┬────┘ └─┬──────┘ └┬───────┘
     │        │         │
┌────▼────┐ ┌─▼──────┐ ┌▼───────┐
│Replica1 │ │Replica2│ │Replica3│  Replicas
└─────────┘ └────────┘ └────────┘
```

## Components

- **Nginx**: Load balancer distributing requests across proxy servers (least_conn algorithm)
- **Proxy (3 instances)**: Spring Boot applications with consistent hashing for shard routing (150 virtual nodes)
- **Cache Nodes (6 total)**:
  - 3 Master nodes (one per shard) - 2GB heap each
  - 3 Replica nodes (one replica per master) - 1.5GB heap each
  - Asynchronous master-replica replication
- **Monitoring Stack**:
  - Prometheus: Metrics collection (15s scrape interval)
  - Grafana: Visualization and dashboards (pre-configured)
  - Loki: Log aggregation (7-day retention)

## Features

- **Low Latency**: ZGC garbage collector (<1ms pause times)
- **Zero-Copy I/O**: Netty pooled direct buffers with Unsafe
- **High Availability**: Master-replica replication across nodes
- **Scalability**: Consistent hashing with minimal data movement
- **Observability**: Full metrics, logs, and dashboards out of the box

## Prerequisites

### Local Development
- Docker 20.10+
- Docker Compose 2.0+
- At least 8GB RAM available
- Ports: 80, 8080-8086, 7000-7005, 7100-7105, 3000, 9090, 3100

### Production (2 VPS Servers)
- 2 VPS servers with 4 vCPU and 8GB RAM each
- Ubuntu 20.04+ or similar
- Docker 20.10+ and Docker Compose 2.0+
- Network connectivity between servers
- See [DEPLOYMENT.md](DEPLOYMENT.md) for detailed setup

## Quick Start (Local Development)

### 1. Setup Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env if needed (optional for local development)
# Default values work for local testing
nano .env
```

### 2. Build and Start All Services

```bash
# Build and start all 13 services
docker-compose up --build -d

# View logs
docker-compose logs -f

# Check status (should show 13 containers running)
docker-compose ps

# View specific service logs
docker-compose logs -f cache-master-1
docker-compose logs -f proxy-1
```

### 3. Wait for Services to Start

First startup takes 2-3 minutes:
- Maven dependencies download
- Docker image builds
- Service initialization

```bash
# Watch all services come up
watch docker-compose ps
```

### 4. Verify Deployment

```bash
# Check Nginx health
curl http://localhost/health
# Expected: "healthy"

# Test cache operations via Nginx load balancer
curl -X PUT http://localhost/cache/test-key \
  -H "Content-Type: application/json" \
  -d '{"value":"hello-world","ttl":3600}'

# Retrieve cached value
curl http://localhost/cache/test-key
# Expected: {"value":"hello-world"}

# Delete cached value
curl -X DELETE http://localhost/cache/test-key
```

### 5. Access Monitoring Dashboards

- **Grafana**: http://localhost:3000
  - Username: `admin`
  - Password: `admin` (change on first login)
  - Pre-configured dashboards for cache nodes, proxies, and replication

- **Prometheus**: http://localhost:9090
  - Query metrics directly
  - Example: `cache_hits_total`

- **Prometheus Targets**: http://localhost:9090/targets
  - Check all services are being scraped

### 6. Stop Services

```bash
# Stop all services (keeps data volumes)
docker-compose down

# Stop and remove volumes (deletes all data)
docker-compose down -v

# Stop specific service
docker-compose stop cache-master-1
```

## Configuration

### Environment Variables

Key variables in `.env`:

- `COMPOSE_PROJECT_NAME`: Docker Compose project name (default: distributed-cache)
- `REGISTRY`: Docker registry for production images (e.g., docker.io/username)
- `TAG`: Image tag for versioning (default: latest)
- `NGINX_PORT`: Nginx HTTP port (default: 80)
- `PROXY_*_PORT`: Proxy service ports (8080-8082)
- `CACHE_NODES`: Master nodes connection string (format: id:host:port:replication_port)
- `GRAFANA_ADMIN_USER/PASSWORD`: Grafana credentials (default: admin/admin)

See `.env.example` for full list and descriptions.

### JVM Configuration

All Java applications support `JAVA_OPTS` environment variable:

**Production settings (in docker-compose.prod.yml):**
- **Proxy**: 512MB heap, 128MB direct memory, ZGC
- **Cache Masters**: 2GB heap, 512MB direct memory, ZGC
- **Cache Replicas**: 1.5GB heap, 384MB direct memory, ZGC

**Key JVM flags:**
- `-XX:+UseZGC -XX:+ZGenerational`: Ultra-low latency GC (<1ms pauses)
- `-XX:MaxDirectMemorySize`: Netty direct buffer memory
- `-Dio.netty.allocator.type=pooled`: Pooled ByteBuf allocator
- `-XX:+AlwaysPreTouch`: Pre-fault memory for consistent performance

### Cache Node Configuration

Each cache node accepts these environment variables:

- `NODE_ID`: Unique identifier (e.g., master-1, replica-1)
- `NODE_PORT`: Client connection port (7000-7005)
- `REPLICATION_PORT`: Master-replica sync port (7100-7105)
- `METRICS_PORT`: Prometheus metrics port (8081-8086)
- `IS_MASTER`: true/false (master or replica)
- `MASTER_HOST`: (replicas only) Master hostname for replication
- `MASTER_PORT`: (replicas only) Master replication port
- `JAVA_OPTS`: JVM tuning parameters (see docker-compose.prod.yml)

## Development

### Building Images

```bash
# Build specific service
docker-compose build cache-node
docker-compose build proxy

# Build all
docker-compose build
```

### Running Tests

```bash
# Run tests before building
mvn clean test

# Run integration tests
mvn verify
```

### Local Development (without Docker)

```bash
# Build all modules
mvn clean package

# Run cache node
java -jar cache-node/target/cache-node-1.0-SNAPSHOT.jar

# Run proxy (in another terminal)
cd proxy
mvn spring-boot:run
```

## Monitoring

### Grafana Dashboards

Access Grafana at `http://localhost:3000` (default: admin/admin)

Pre-configured dashboards:
- **Proxy Dashboard**: Request rates, latency, error rates
- **Cache Node Dashboard**: Hit/miss ratios, memory usage, operations
- **Network Dashboard**: Network I/O, connection pools
- **Replication Dashboard**: Replication lag, sync status

### Prometheus Metrics

Available at `http://localhost:9090`

Key metrics:
- `cache_requests_total`: Total cache requests
- `cache_hits_total`: Cache hits
- `cache_misses_total`: Cache misses
- `replication_lag_seconds`: Replication delay
- `proxy_routing_duration_seconds`: Routing latency

### Logs

View logs with Loki integration in Grafana:
- Filter by service: `{container="cache-master-1"}`
- Filter by level: `{level="ERROR"}`

## Production Deployment

### Two-Server Setup

This project is optimized for deployment on 2 VPS servers with 4 vCPU and 8GB RAM each.

**Server 1 hosts:**
- Nginx load balancer
- Proxy-1, Proxy-2
- Cache-Master-1, Cache-Master-2
- Cache-Replica-3
- Prometheus

**Server 2 hosts:**
- Proxy-3
- Cache-Master-3
- Cache-Replica-1, Cache-Replica-2
- Grafana, Loki

### Deployment Steps

For detailed step-by-step deployment instructions, see [DEPLOYMENT.md](DEPLOYMENT.md).

**Quick summary:**

1. **Build and push images** to Docker registry:
```bash
export REGISTRY=docker.io/your-username
export TAG=v1.0.0

docker-compose build
docker tag distributed-cache-proxy:latest ${REGISTRY}/cache-proxy:${TAG}
docker tag distributed-cache-cache-node:latest ${REGISTRY}/cache-node:${TAG}
docker push ${REGISTRY}/cache-proxy:${TAG}
docker push ${REGISTRY}/cache-node:${TAG}
```

2. **Deploy on Server 1** (see DEPLOYMENT.md for service filtering):
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

3. **Deploy on Server 2** (see DEPLOYMENT.md for service filtering):
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

### Production Features

- **Optimized JVM**: ZGC with generational mode for <1ms GC pauses
- **Netty Direct Buffers**: Zero-copy I/O using pooled allocators
- **Resource Limits**: CPU and memory limits tuned for 8GB RAM
- **Log Rotation**: Automatic log rotation (10MB max, 3 files)
- **Health Checks**: Docker health checks for all services
- **Restart Policies**: Automatic restart on failure

### Production Considerations

- **SSL/TLS**: Add SSL certificates to nginx configuration for HTTPS
- **Firewall**: Configure firewall rules between servers
- **Backup**: Regularly backup Docker volumes (`/data` directories)
- **Monitoring**: Set up Grafana alerts for critical metrics
- **Updates**: Follow zero-downtime update procedures in DEPLOYMENT.md

## Troubleshooting

### Services not starting

```bash
# Check logs
docker-compose logs [service-name]

# Restart specific service
docker-compose restart cache-master-1

# Rebuild and restart
docker-compose up -d --build --force-recreate
```

### Port conflicts

```bash
# Check port usage
netstat -tulpn | grep LISTEN

# Change ports in .env file
nano .env
docker-compose down
docker-compose up -d
```

### Network issues

```bash
# Recreate network
docker-compose down
docker network prune
docker-compose up -d
```

## API Reference

### Cache Operations

#### PUT - Store Value
```bash
curl -X PUT http://localhost/cache/{key} \
  -H "Content-Type: application/json" \
  -d '{"value":"your-value","ttl":3600}'
```

#### GET - Retrieve Value
```bash
curl http://localhost/cache/{key}
```

#### DELETE - Remove Value
```bash
curl -X DELETE http://localhost/cache/{key}
```

### Health Checks

```bash
# Nginx health
curl http://localhost/health

# Proxy health
curl http://localhost:8080/actuator/health

# Metrics
curl http://localhost:8080/actuator/prometheus
```

## Architecture Decisions

### Consistent Hashing
- Uses virtual nodes (150 per physical node) for even distribution
- Minimal data movement when adding/removing nodes
- Implemented in proxy layer for client transparency
- Hash function ensures uniform key distribution across shards

### Master-Replica Replication
- Asynchronous replication for performance
- Each shard has one master, one replica
- Replication lag typically <100ms
- Manual failover (promote replica to master when needed)

### Monitoring Stack
- Prometheus for metrics (15s scrape interval)
- Grafana for visualization with pre-built dashboards
- Loki for centralized logging (7-day retention)
- Metrics exported via Micrometer (Spring Boot) and custom exporters

### JVM Optimization
- **ZGC**: Chosen for ultra-low pause times (<1ms) crucial for caching
- **Generational ZGC**: Better throughput than classic ZGC
- **Direct Buffers**: Netty uses off-heap memory for zero-copy I/O
- **Pooled Allocators**: Reduces object allocation pressure on GC

## Performance Tuning

### JVM Settings

Modify `JAVA_OPTS` in docker-compose.prod.yml for tuning:

```yaml
environment:
  # For higher throughput (more CPU overhead)
  - JAVA_OPTS=-Xmx2g -Xms2g -XX:+UseZGC -XX:ConcGCThreads=2

  # For lower latency (current default)
  - JAVA_OPTS=-Xmx2g -Xms2g -XX:+UseZGC -XX:+ZGenerational

  # For debugging
  - JAVA_OPTS=-Xmx2g -Xms2g -XX:+UseZGC -Xlog:gc*:stdout:time
```

### Connection Pooling

Adjust in `.env`:

```bash
CACHE_POOL_SIZE=20              # Connections per proxy to each cache node
CACHE_TIMEOUT_MS=5000           # Operation timeout
CACHE_MAX_RETRIES=3             # Retry attempts on failure
CACHE_VIRTUAL_NODES=150         # Virtual nodes in consistent hash ring
```

### Memory Allocation

Tune direct memory based on workload:

```bash
# High write throughput (more direct buffers)
-XX:MaxDirectMemorySize=1024m

# High read throughput (larger heap cache)
-Xmx4g -XX:MaxDirectMemorySize=256m
```

### Expected Performance

**Latency (local network):**
- P50: <5ms
- P99: <20ms
- P99.9: <50ms

**Throughput (per proxy):**
- Read: ~50,000 ops/sec
- Write: ~30,000 ops/sec
- Mixed (70/30 read/write): ~40,000 ops/sec

**GC Impact:**
- Pause times: <1ms (ZGC)
- Frequency: Every 30-60 seconds (depends on load)

## Project Structure

```
distributed-cache/
├── cache-node/              # Cache node service (master/replica)
│   ├── src/                 # Java source code
│   ├── Dockerfile           # Multi-stage build with Java 25
│   └── pom.xml              # Maven dependencies
├── proxy/                   # Proxy service (Spring Boot)
│   ├── src/                 # Java source code
│   ├── Dockerfile           # Multi-stage build with Java 25
│   └── pom.xml              # Maven dependencies
├── common/                  # Shared libraries
│   └── src/                 # Common utilities and DTOs
├── monitoring/              # Monitoring configurations
│   ├── prometheus.yml       # Prometheus scrape config
│   ├── grafana-datasources.yml
│   ├── grafana-dashboards.yml
│   ├── loki-config.yml
│   └── dashboards/          # Pre-built Grafana dashboards
├── nginx/                   # Nginx load balancer config
│   └── nginx.conf           # Upstream and routing rules
├── docker-compose.yml       # Base compose file (all services)
├── docker-compose.prod.yml  # Production overrides (resource limits, JVM tuning)
├── .env.example             # Environment variables template
├── README.md                # This file
└── DEPLOYMENT.md            # Detailed deployment guide

Total services: 13 containers
├── 1 Nginx load balancer
├── 3 Proxy instances
├── 3 Cache masters
├── 3 Cache replicas
└── 3 Monitoring services (Prometheus, Grafana, Loki)
```

## Technologies

- **Java 25**: Latest LTS with ZGC improvements
- **Spring Boot 3.5.6**: Modern Spring stack for proxy
- **Netty**: High-performance async I/O for cache nodes
- **Docker & Docker Compose**: Containerization and orchestration
- **Nginx**: Load balancing and reverse proxy
- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and alerting
- **Loki**: Log aggregation
- **Maven**: Build tool and dependency management

## License

MIT License - See LICENSE file for details

## Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Run tests: `mvn test`
4. Submit a pull request

## Support

For issues and questions:
- GitHub Issues: https://github.com/Vonaski/distributed-cache/issues
- Documentation: See README.md and DEPLOYMENT.md
- Monitoring: Check Grafana dashboards for system health
