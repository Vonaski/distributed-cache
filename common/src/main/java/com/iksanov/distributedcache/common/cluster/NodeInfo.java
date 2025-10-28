package com.iksanov.distributedcache.common.cluster;

import java.util.Objects;

public record NodeInfo(String nodeId, String host, int port, int replicationPort) {

    public NodeInfo {
        if (nodeId == null || nodeId.isBlank()) throw new IllegalArgumentException("nodeId cannot be null or blank");
        if (host == null || host.isBlank()) throw new IllegalArgumentException("host cannot be null or blank");
        if (port <= 0 || port > 65535) throw new IllegalArgumentException("port out of range");
        if (replicationPort <= 0 || replicationPort > 65535) throw new IllegalArgumentException("replicationPort out of range");
    }

    public NodeInfo(String nodeId, String host, int port) {
        this(nodeId, host, port, port + 1000);
    }

    public int replicationPort() {
        return replicationPort;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeInfo that)) return false;
        return Objects.equals(this.nodeId, that.nodeId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId);
    }

    @Override
    public String toString() {
        return "NodeInfo[nodeId=%s, host=%s, port=%d, replicationPort=%d]".formatted(nodeId, host, port, replicationPort);
    }
}