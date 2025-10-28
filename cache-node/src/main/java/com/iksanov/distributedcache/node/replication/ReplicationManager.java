package com.iksanov.distributedcache.node.replication;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.Function;

/**
 * Coordinates replication for this node.
 * <p>
 * Design notes:
 * <ul>
 *   <li>{@code ReplicaManager} (from common. Cluster) stores mapping master → replicas and replica → master,
 *       but does not resolve which master is responsible for a key.</li>
 *   <li>Therefore ReplicationManager accepts a {@link java.util.function.Function Function&lt;String, NodeInfo&gt;}
 *       primaryResolver, which resolves the primary/master node for a key (for example, using ConsistentHashRing).</li>
 * </ul>
 */
public class ReplicationManager {

    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);
    private final NodeInfo currentNode;
    private final ReplicationSender sender;
    private final ReplicationReceiver receiver;
    private final Function<String, NodeInfo> primaryResolver;

    public ReplicationManager(NodeInfo currentNode,
                              ReplicationSender sender,
                              ReplicationReceiver receiver,
                              Function<String, NodeInfo> primaryResolver) {
        this.currentNode = Objects.requireNonNull(currentNode, "currentNode");
        this.sender = Objects.requireNonNull(sender, "sender");
        this.receiver = Objects.requireNonNull(receiver, "receiver");
        this.primaryResolver = Objects.requireNonNull(primaryResolver, "primaryResolver");
    }

    private boolean isMasterForKey(String key) {
        NodeInfo master = primaryResolver.apply(key);
        if (master == null) {
            log.warn("Primary resolver returned null for key={}, skipping replication", key);
            return false;
        }
        if (!currentNode.equals(master)) {
            log.debug("This node ({}) is not master for key={}, master={}, skip replication",
                    currentNode.nodeId(), key, master.nodeId());
            return false;
        }
        return true;
    }

    public void onLocalSet(String key, String value) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(value, "value");
        if (!isMasterForKey(key)) return;

        ReplicationTask task = ReplicationTask.ofSet(key, value, currentNode.nodeId());
        sender.replicate(currentNode, task);
        log.debug("Triggered replication SET key={} from master={}", key, currentNode.nodeId());
    }

    public void onLocalDelete(String key) {
        Objects.requireNonNull(key, "key");
        if (!isMasterForKey(key)) return;

        ReplicationTask task = ReplicationTask.ofDelete(key, currentNode.nodeId());
        sender.replicate(currentNode, task);
        log.debug("Triggered replication DELETE key={} from master={}", key, currentNode.nodeId());
    }

    public void onReplicationReceived(ReplicationTask task) {
        try {
            log.info("Applying replicated {} for key={} (origin={}, ts={})",
                    task.operation(), task.key(), task.origin(), task.timestamp());
            receiver.applyTask(task);
        } catch (Exception e) {
            log.error("Failed to apply incoming replication task: {}", task, e);
        }
    }

    public void shutdown() {
        try {
            sender.shutdown();
        } catch (Exception e) {
            log.warn("Error while shutting down replication sender", e);
        }
    }
}
