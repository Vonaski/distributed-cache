package com.iksanov.distributedcache.node.replication.failover;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.node.config.ApplicationConfig.NodeRole;
import com.iksanov.distributedcache.node.replication.ReplicationSender;
import org.junit.jupiter.api.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for {@link FailoverManager}.
 * <p>
 * Covers:
 *  - Master heartbeat sending
 *  - Replica heartbeat monitoring and timeout detection
 *  - Promotion from replica to master
 *  - Role change listener callbacks
 *  - Start/stop lifecycle
 *  - Edge cases and null handling
 */
class FailoverManagerTest {

    private ReplicationSender replicationSender;
    private NodeInfo masterNode;
    private NodeInfo replicaNode;
    private FailoverManager failoverManager;

    @BeforeEach
    void setUp() {
        replicationSender = mock(ReplicationSender.class);
        masterNode = new NodeInfo("master-1", "127.0.0.1", 9001);
        replicaNode = new NodeInfo("replica-1", "127.0.0.1", 9002);
    }

    @AfterEach
    void tearDown() {
        if (failoverManager != null && failoverManager.isRunning()) failoverManager.stop();
    }

    @Test
    @DisplayName("Master should send periodic heartbeats to replicas")
    void masterShouldSendPeriodicHeartbeats() throws InterruptedException {
        failoverManager = new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                replicationSender,
                masterNode,
                null
        );
        failoverManager.start();
        Thread.sleep(2500);
        verify(replicationSender, atLeast(2)).sendHeartbeatToReplicas(masterNode);
    }

    @Test
    @DisplayName("Replica should not promote to master when receiving regular heartbeats")
    void replicaShouldNotPromoteWhenReceivingHeartbeats() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );
        FailoverManager.RoleChangeListener listener = mock(FailoverManager.RoleChangeListener.class);
        failoverManager.setRoleChangeListener(listener);
        failoverManager.start();
        for (int i = 0; i < 5; i++) {
            failoverManager.onReplicationReceived();
            Thread.sleep(500);
        }
        assertEquals(NodeRole.REPLICA, failoverManager.getCurrentRole());
        verify(listener, never()).onRoleChanged(any(), any(), anyLong());
    }

    @Test
    @DisplayName("Replica should promote to master after heartbeat timeout")
    void replicaShouldPromoteAfterHeartbeatTimeout() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );

        CountDownLatch promotionLatch = new CountDownLatch(1);
        failoverManager.setRoleChangeListener((oldRole, newRole, epoch) -> {
            assertEquals(NodeRole.REPLICA, oldRole);
            assertEquals(NodeRole.MASTER, newRole);
            assertTrue(epoch > 0);
            promotionLatch.countDown();
        });
        failoverManager.start();
        boolean promoted = promotionLatch.await(5, TimeUnit.SECONDS);
        assertTrue(promoted, "Replica should have been promoted to master");
        assertEquals(NodeRole.MASTER, failoverManager.getCurrentRole());
        assertTrue(failoverManager.getEpoch() > 0);
    }

    @Test
    @DisplayName("Replica with priority != 1 should not monitor heartbeat")
    void replicaWithLowPriorityShouldNotMonitor() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-2",
                2,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );

        FailoverManager.RoleChangeListener listener = mock(FailoverManager.RoleChangeListener.class);
        failoverManager.setRoleChangeListener(listener);
        failoverManager.start();
        Thread.sleep(4000);
        assertEquals(NodeRole.REPLICA, failoverManager.getCurrentRole());
        verify(listener, never()).onRoleChanged(any(), any(), anyLong());
    }

    @Test
    @DisplayName("onReplicationReceived should update heartbeat timestamp")
    void onReplicationReceivedShouldUpdateTimestamp() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );

        CountDownLatch noPromotionLatch = new CountDownLatch(1);
        failoverManager.setRoleChangeListener((oldRole, newRole, epoch) -> noPromotionLatch.countDown());
        failoverManager.start();
        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            failoverManager.onReplicationReceived();
        }

        assertFalse(noPromotionLatch.await(100, TimeUnit.MILLISECONDS), "Should not promote when receiving regular heartbeats");
        assertEquals(NodeRole.REPLICA, failoverManager.getCurrentRole());
    }

    @Test
    @DisplayName("onHeartbeatAck should update heartbeat timestamp")
    void onHeartbeatAckShouldUpdateTimestamp() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );

        CountDownLatch noPromotionLatch = new CountDownLatch(1);
        failoverManager.setRoleChangeListener((oldRole, newRole, epoch) -> noPromotionLatch.countDown());
        failoverManager.start();
        for (int i = 0; i < 5; i++) {
            Thread.sleep(1000);
            failoverManager.onHeartbeatAck(1L);
        }
        assertFalse(noPromotionLatch.await(100, TimeUnit.MILLISECONDS), "Should not promote when receiving regular heartbeat ACKs");
        assertEquals(NodeRole.REPLICA, failoverManager.getCurrentRole());
    }

    @Test
    @DisplayName("Promoted replica should start sending heartbeats as master")
    void promotedReplicaShouldStartSendingHeartbeats() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );

        CountDownLatch promotionLatch = new CountDownLatch(1);
        failoverManager.setRoleChangeListener((oldRole, newRole, epoch) -> promotionLatch.countDown());
        failoverManager.start();
        assertTrue(promotionLatch.await(5, TimeUnit.SECONDS), "Should promote to master");
        Thread.sleep(2500);
        verify(replicationSender, atLeast(1)).sendHeartbeatToReplicas(replicaNode);
    }

    @Test
    @DisplayName("stop() should shutdown scheduler gracefully")
    void stopShouldShutdownScheduler() {
        failoverManager = new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                replicationSender,
                masterNode,
                null
        );

        failoverManager.start();
        assertTrue(failoverManager.isRunning());
        failoverManager.stop();
        assertFalse(failoverManager.isRunning());
    }

    @Test
    @DisplayName("Multiple start() calls should be idempotent")
    void multipleStartCallsShouldBeIdempotent() {
        failoverManager = new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                replicationSender,
                masterNode,
                null
        );

        failoverManager.start();
        assertTrue(failoverManager.isRunning());
        failoverManager.start();
        assertTrue(failoverManager.isRunning());
    }

    @Test
    @DisplayName("Multiple stop() calls should be idempotent")
    void multipleStopCallsShouldBeIdempotent() {
        failoverManager = new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                replicationSender,
                masterNode,
                null
        );

        failoverManager.start();
        failoverManager.stop();
        assertFalse(failoverManager.isRunning());
        assertDoesNotThrow(() -> failoverManager.stop());
        assertFalse(failoverManager.isRunning());
    }

    @Test
    @DisplayName("Constructor should require non-null nodeId")
    void constructorShouldRequireNonNullNodeId() {
        assertThrows(NullPointerException.class, () -> new FailoverManager(
                null,
                0,
                NodeRole.MASTER,
                replicationSender,
                masterNode,
                null
        ));
    }

    @Test
    @DisplayName("Constructor should require non-null initialRole")
    void constructorShouldRequireNonNullInitialRole() {
        assertThrows(NullPointerException.class, () -> new FailoverManager(
                "master-1",
                0,
                null,
                replicationSender,
                masterNode,
                null
        ));
    }

    @Test
    @DisplayName("Constructor should require non-null replicationSender")
    void constructorShouldRequireNonNullReplicationSender() {
        assertThrows(NullPointerException.class, () -> new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                null,
                masterNode,
                null
        ));
    }

    @Test
    @DisplayName("Replica created without masterNode should still work but log warning")
    void replicaWithoutMasterNodeShouldWork() {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                null
        );
        assertDoesNotThrow(() -> failoverManager.start());
        assertTrue(failoverManager.isRunning());
    }

    @Test
    @DisplayName("Master created without selfNode should still work but log warning")
    void masterWithoutSelfNodeShouldWork() {
        failoverManager = new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                replicationSender,
                null,
                null
        );
        assertDoesNotThrow(() -> failoverManager.start());
        assertTrue(failoverManager.isRunning());
    }

    @Test
    @DisplayName("Epoch should increment on promotion")
    void epochShouldIncrementOnPromotion() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );

        assertEquals(0, failoverManager.getEpoch());
        CountDownLatch promotionLatch = new CountDownLatch(1);
        failoverManager.setRoleChangeListener((oldRole, newRole, epoch) -> {
            assertEquals(1L, epoch);
            promotionLatch.countDown();
        });
        failoverManager.start();
        assertTrue(promotionLatch.await(5, TimeUnit.SECONDS));
        assertEquals(1, failoverManager.getEpoch());
    }

    @Test
    @DisplayName("Role change listener exception should not crash failover manager")
    void listenerExceptionShouldNotCrashManager() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );

        CountDownLatch listenerCalledLatch = new CountDownLatch(1);
        failoverManager.setRoleChangeListener((oldRole, newRole, epoch) -> {
            listenerCalledLatch.countDown();
            throw new RuntimeException("Listener exception for testing");
        });
        failoverManager.start();
        assertTrue(listenerCalledLatch.await(5, TimeUnit.SECONDS));
        assertEquals(NodeRole.MASTER, failoverManager.getCurrentRole());
        assertTrue(failoverManager.isRunning());
    }

    @Test
    @DisplayName("getCurrentRole should return current role")
    void getCurrentRoleShouldReturnCurrentRole() {
        failoverManager = new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                replicationSender,
                masterNode,
                null
        );
        assertEquals(NodeRole.MASTER, failoverManager.getCurrentRole());
    }

    @Test
    @DisplayName("isRunning should return false before start")
    void isRunningShouldReturnFalseBeforeStart() {
        failoverManager = new FailoverManager(
                "master-1",
                0,
                NodeRole.MASTER,
                replicationSender,
                masterNode,
                null
        );
        assertFalse(failoverManager.isRunning());
    }

    @Test
    @DisplayName("Already promoted replica should not promote again")
    void alreadyPromotedReplicaShouldNotPromoteAgain() throws InterruptedException {
        failoverManager = new FailoverManager(
                "replica-1",
                1,
                NodeRole.REPLICA,
                replicationSender,
                replicaNode,
                masterNode
        );
        CountDownLatch promotionLatch = new CountDownLatch(1);
        failoverManager.setRoleChangeListener((oldRole, newRole, epoch) -> promotionLatch.countDown());
        failoverManager.start();
        assertTrue(promotionLatch.await(5, TimeUnit.SECONDS));
        assertEquals(NodeRole.MASTER, failoverManager.getCurrentRole());
        long firstEpoch = failoverManager.getEpoch();
        Thread.sleep(2000);
        assertEquals(NodeRole.MASTER, failoverManager.getCurrentRole());
        assertEquals(firstEpoch, failoverManager.getEpoch(), "Epoch should not change after already promoted");
    }
}
