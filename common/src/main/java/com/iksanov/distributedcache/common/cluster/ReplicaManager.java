package com.iksanov.distributedcache.common.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ReplicaManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicaManager.class);
    private final ConcurrentMap<String, Set<NodeInfo>> masterToReplicas = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NodeInfo> replicaToMaster = new ConcurrentHashMap<>();

    public void registerReplica(NodeInfo master, NodeInfo replica) {
        Objects.requireNonNull(master, "master is null");
        Objects.requireNonNull(replica, "replica is null");

        masterToReplicas.computeIfAbsent(master.nodeId(),
                        k -> ConcurrentHashMap.newKeySet())
                .add(replica);
        replicaToMaster.put(replica.nodeId(), master);

        log.debug("Registered replica {} for master {}", replica.nodeId(), master.nodeId());
    }

    public Set<NodeInfo> getReplicas(NodeInfo master) {
        var set = masterToReplicas.get(master.nodeId());
        return set == null ? Collections.emptySet() : Collections.unmodifiableSet(new HashSet<>(set));
    }

    public NodeInfo getMaster(NodeInfo replica) {
        return replicaToMaster.get(replica.nodeId());
    }

    public void removeMaster(NodeInfo master) {
        Objects.requireNonNull(master, "master is null");
        var removed = masterToReplicas.remove(master.nodeId());
        if (removed != null) {
            removed.forEach(r -> replicaToMaster.remove(r.nodeId()));
            log.debug("Removed master {} and its {} replicas", master.nodeId(), removed.size());
        }
    }

    public void removeReplica(NodeInfo replica) {
        Objects.requireNonNull(replica, "replica is null");
        NodeInfo master = replicaToMaster.remove(replica.nodeId());
        if (master != null) {
            masterToReplicas.computeIfPresent(master.nodeId(), (k, set) -> {
                set.remove(replica);
                return set;
            });
            log.debug("Removed replica {} from master {}", replica.nodeId(), master.nodeId());
        }
    }

    public void clear() {
        masterToReplicas.clear();
        replicaToMaster.clear();
    }

    public Set<String> getAllMasters() {
        return Collections.unmodifiableSet(masterToReplicas.keySet());
    }
}
