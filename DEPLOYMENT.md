# Deployment Guide

## Overview

This guide explains how to set up CI/CD for deploying the distributed cache system to 2 VPS servers using GitHub Actions.

## Architecture

- **VPS 1**: Nginx, Proxy-1, Proxy-2, Cache Master-1, Master-2, Replica-3, Prometheus
- **VPS 2**: Proxy-3, Cache Master-3, Replica-1, Replica-2, Grafana, Loki

## Prerequisites

1. **2 VPS servers** with:
   - Ubuntu 20.04+ or similar Linux distribution
   - Docker and Docker Compose installed
   - SSH access configured
   - Minimum 4 vCPU and 8GB RAM each

2. **GitHub repository** with admin access

3. **GitHub Personal Access Token (PAT)** with `write:packages` permission (you already created this)

## Step-by-Step Setup

### 1. Prepare VPS Servers

On **both VPS servers**, run the following commands:

```bash
# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker --version
docker-compose --version

# Create deployment directory
mkdir -p ~/distributed-cache
```

### 2. Configure SSH Access for GitHub Actions

On your **local machine**, generate an SSH key pair for deployment:

```bash
# Generate SSH key pair (without passphrase)
ssh-keygen -t ed25519 -C "github-actions-deploy" -f ~/.ssh/github_actions_deploy -N ""

# This creates:
# - Private key: ~/.ssh/github_actions_deploy
# - Public key: ~/.ssh/github_actions_deploy.pub
```

Copy the **public key** to both VPS servers:

```bash
# For VPS1
ssh-copy-id -i ~/.ssh/github_actions_deploy.pub user@VPS1_IP

# For VPS2
ssh-copy-id -i ~/.ssh/github_actions_deploy.pub user@VPS2_IP

# Or manually: copy content of github_actions_deploy.pub and paste it to ~/.ssh/authorized_keys on each VPS
```

### 3. Configure GitHub Secrets

Go to your GitHub repository → **Settings** → **Secrets and variables** → **Actions** → **New repository secret**

Add the following secrets:

| Secret Name | Value | Description |
|------------|-------|-------------|
| `VPS1_HOST` | `123.45.67.89` | IP address or domain of VPS 1 |
| `VPS1_USER` | `ubuntu` | SSH username for VPS 1 |
| `VPS1_SSH_KEY` | `<private_key_content>` | Content of `~/.ssh/github_actions_deploy` (private key) |
| `VPS1_SSH_PORT` | `22` | SSH port for VPS 1 (usually 22) |
| `VPS2_HOST` | `98.76.54.32` | IP address or domain of VPS 2 |
| `VPS2_USER` | `ubuntu` | SSH username for VPS 2 |
| `VPS2_SSH_KEY` | `<private_key_content>` | Same private key as VPS1 |
| `VPS2_SSH_PORT` | `22` | SSH port for VPS 2 (usually 22) |
| `GRAFANA_PASSWORD` | `<secure_password>` | Admin password for Grafana |

**To get the private key content:**

```bash
# Display the private key
cat ~/.ssh/github_actions_deploy

# Copy the entire output including:
# -----BEGIN OPENSSH PRIVATE KEY-----
# ... key content ...
# -----END OPENSSH PRIVATE KEY-----
```

### 4. Configure Firewall Rules

On **VPS 1**, open the following ports:

```bash
# HTTP traffic (Nginx)
sudo ufw allow 80/tcp

# Cache node client ports
sudo ufw allow 7000:7002/tcp

# Cache node replication ports
sudo ufw allow 7100:7102/tcp

# Prometheus
sudo ufw allow 9090/tcp

# Allow VPS2 to connect
sudo ufw allow from VPS2_IP

# Enable firewall
sudo ufw enable
```

On **VPS 2**, open the following ports:

```bash
# Proxy port
sudo ufw allow 8082/tcp

# Cache node ports
sudo ufw allow 7002:7005/tcp

# Replication ports
sudo ufw allow 7102:7105/tcp

# Grafana
sudo ufw allow 3000/tcp

# Allow VPS1 to connect
sudo ufw allow from VPS1_IP

# Enable firewall
sudo ufw enable
```

### 5. Configure GitHub Container Registry

Enable GHCR for your repository:

1. Go to **Settings** → **Actions** → **General**
2. Scroll to **Workflow permissions**
3. Select **Read and write permissions**
4. Check **Allow GitHub Actions to create and approve pull requests**
5. Click **Save**

### 6. Test the CI/CD Pipeline

#### Test 1: CI Pipeline (tests + build)

```bash
# Create a new branch
git checkout -b test-ci

# Make a small change
echo "# Test" >> README.md

# Commit and push
git add .
git commit -m "test: trigger CI pipeline"
git push origin test-ci

# Create a Pull Request on GitHub
# The CI pipeline should automatically run tests and build Docker images
```

Check the **Actions** tab in GitHub to see the pipeline running.

#### Test 2: Deployment Pipeline

```bash
# After CI passes, merge the PR to main branch
# The deployment pipeline will automatically trigger and deploy to both VPS servers
```

### 7. Verify Deployment

After deployment completes, verify the services:

```bash
# Check VPS1 health
curl http://VPS1_IP/health

# Test cache operations
curl -X PUT http://VPS1_IP/api/cache/test-key \
  -H "Content-Type: application/json" \
  -d '{"value":"test-value"}'

curl http://VPS1_IP/api/cache/test-key

# Access monitoring
# Prometheus: http://VPS1_IP:9090
# Grafana: http://VPS2_IP:3000 (login: admin / <GRAFANA_PASSWORD>)
```

## CI/CD Pipeline Details

### CI Pipeline (`.github/workflows/ci.yml`)

Triggered on:
- Push to `develop` or `main` branches
- Pull requests to `develop` or `main` branches
- Manual workflow dispatch

Jobs:
1. **Test**: Run unit tests (excludes Stress, Load, Resilience tests)
2. **Build**: Build Maven artifacts
3. **Docker Build**: Build and push Docker images to GHCR
4. **Security Scan**: Scan Docker images for vulnerabilities using Trivy

### Deploy Pipeline (`.github/workflows/deploy.yml`)

Triggered on:
- PR merged to `main` branch
- Manual workflow dispatch

Jobs:
1. **Deploy VPS1**: Deploy Nginx, Proxy-1, Proxy-2, Cache nodes, Prometheus
2. **Deploy VPS2**: Deploy Proxy-3, Cache nodes, Grafana, Loki
3. **Verify**: Health checks and basic cache operation tests

## Test Configuration

Tests excluded from CI:
- `**/*StressTest.java` - Stress tests (long-running)
- `**/*LoadTest.java` - Load tests (long-running)
- `**/*ResilienceTest.java` - Resilience tests (long-running)

To run all tests locally:

```bash
mvn clean test
```

To run only fast tests (same as CI):

```bash
mvn clean test -Dtest='!**/*StressTest,!**/*LoadTest,!**/*ResilienceTest'
```

## Manual Deployment

If you need to deploy manually:

1. Go to **Actions** tab in GitHub
2. Select **Deploy to Production** workflow
3. Click **Run workflow**
4. Select branch and environment
5. Click **Run workflow**

## Troubleshooting

### Issue: SSH connection fails

```bash
# On your local machine, test SSH connection
ssh -i ~/.ssh/github_actions_deploy user@VPS_IP

# Check SSH key permissions
chmod 600 ~/.ssh/github_actions_deploy
chmod 644 ~/.ssh/github_actions_deploy.pub
```

### Issue: Docker pull fails on VPS

```bash
# On VPS, login to GHCR manually
echo "YOUR_GITHUB_TOKEN" | docker login ghcr.io -u YOUR_GITHUB_USERNAME --password-stdin

# Pull image manually to test
docker pull ghcr.io/YOUR_USERNAME/cache-node:latest
```

### Issue: Containers won't start

```bash
# On VPS, check container logs
cd ~/distributed-cache
docker-compose logs -f

# Check container status
docker-compose ps

# Restart specific service
docker-compose restart cache-master-1
```

### Issue: Network connectivity between VPS servers

```bash
# On VPS1, test connection to VPS2
telnet VPS2_IP 7002

# On VPS2, test connection to VPS1
telnet VPS1_IP 7000

# Check firewall rules
sudo ufw status verbose
```

## Rollback Procedure

If deployment fails and you need to rollback:

1. Find the previous successful deployment SHA from GitHub Actions history
2. Go to **Actions** → **Deploy to Production**
3. Click **Run workflow**
4. In the branch field, enter the commit SHA of the previous version
5. Click **Run workflow**

Or manually on each VPS:

```bash
# On VPS, find previous image tag
docker images | grep cache-node

# Edit .env file and change TAG to previous version
nano ~/distributed-cache/.env

# Redeploy
cd ~/distributed-cache
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
```

## Monitoring

- **Prometheus**: http://VPS1_IP:9090
- **Grafana**: http://VPS2_IP:3000
  - Default credentials: `admin` / `<GRAFANA_PASSWORD>`
  - Dashboards are pre-configured via provisioning

## Security Considerations

1. **Change default passwords**: Update Grafana admin password
2. **Enable HTTPS**: Configure SSL/TLS certificates (see SSL.md for guide)
3. **Restrict access**: Use VPN or IP whitelisting for monitoring interfaces
4. **Rotate SSH keys**: Regularly rotate deployment SSH keys
5. **Update dependencies**: Keep Docker images and base OS updated

## Next Steps

1. **Set up SSL/TLS**: Configure Let's Encrypt for HTTPS
2. **Configure backups**: Set up automated backups for Docker volumes
3. **Add alerting**: Configure Prometheus alerts and notification channels
4. **Performance tuning**: Adjust JVM settings based on production metrics
5. **Disaster recovery**: Document and test recovery procedures
