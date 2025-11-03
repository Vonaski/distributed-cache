package com.iksanov.distributedcache.proxy.health;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.proxy.cluster.ClusterManager;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Component
public class CacheClusterHealthIndicator implements HealthIndicator {

    private final ClusterManager clusterManager;

    public CacheClusterHealthIndicator(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public Health health() {
        try {
            Set<NodeInfo> nodes = clusterManager.getAllNodes();
            if (nodes.isEmpty()) return Health.down().withDetail("reason", "No cache nodes configured").build();
            Map<String, Object> details = new HashMap<>();
            details.put("totalNodes", nodes.size());
            details.put("virtualNodesPerNode", clusterManager.getVirtualNodeCount());
            return Health.up().withDetails(details).build();
        } catch (Exception e) {
            return Health.down().withDetail("error", e.getMessage()).build();
        }
    }
}