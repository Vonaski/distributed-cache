package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.node.replication.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Stress test for {@link ReplicationManager}.
 * <p>
 * Covers:
 *  - concurrent onLocalSet/onLocalDelete calls
 *  - concurrent onReplicationReceived calls
 *  - graceful shutdown under load
 */
public class ReplicationManagerStressTest {

    private ReplicationManager manager;
    private ReplicationSender sender;
    private ReplicationReceiver receiver;
    private NodeInfo self;
    private Function<String, NodeInfo> primaryResolver;

    @BeforeEach
    void setup() {
        self = new NodeInfo("node-A", "127.0.0.1", 9000);
        sender = mock(ReplicationSender.class);
        receiver = mock(ReplicationReceiver.class);
        primaryResolver = key -> self;
        manager = new ReplicationManager(self, sender, receiver, primaryResolver);
    }

    @Test
    void shouldHandleConcurrentLocalOperations() throws InterruptedException {
        int threads = 16;
        int opsPerThread = 2000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger counter = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        String key = "key-" + j;
                        String value = "v-" + j;
                        if (j % 3 == 0) manager.onLocalDelete(key);
                        else manager.onLocalSet(key, value);
                        counter.incrementAndGet();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(60, TimeUnit.SECONDS), "Timeout waiting for workers");
        executor.shutdownNow();
        verify(sender, atLeastOnce()).replicate(eq(self), any(ReplicationTask.class));
        System.out.printf("✅ Completed %d concurrent local operations%n", counter.get());
    }

    @Test
    void shouldHandleConcurrentIncomingReplication() throws InterruptedException {
        int threads = 12;
        int tasksPerThread = 1500;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger received = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < tasksPerThread; j++) {
                        ReplicationTask task = ReplicationTask.ofSet("k-" + j, "val-" + j, "node-B");
                        manager.onReplicationReceived(task);
                        received.incrementAndGet();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }
        assertTrue(latch.await(60, TimeUnit.SECONDS), "Timeout waiting for workers");
        executor.shutdownNow();
        verify(receiver, atLeastOnce()).applyTask(any(ReplicationTask.class));
        System.out.printf("✅ Processed %d incoming replication tasks%n", received.get());
    }

    @Test
    void shouldHandleMixedConcurrentLoad() throws InterruptedException {
        int threads = 20;
        int opsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);
        AtomicInteger total = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < opsPerThread; j++) {
                        String key = "k-" + Thread.currentThread().getId() + "-" + j;
                        if (j % 4 == 0) {
                            ReplicationTask incoming = ReplicationTask.ofSet(key, "v-" + j, "node-B");
                            manager.onReplicationReceived(incoming);
                        } else if (j % 5 == 0) {
                            manager.onLocalDelete(key);
                        } else {
                            manager.onLocalSet(key, "v-" + j);
                        }
                        total.incrementAndGet();
                    }
                } catch (Exception e) {
                    fail("Unexpected exception: " + e.getMessage());
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean done = latch.await(90, TimeUnit.SECONDS);
        executor.shutdownNow();

        assertTrue(done, "Timeout waiting for workers");
        verify(sender, atLeastOnce()).replicate(eq(self), any(ReplicationTask.class));
        verify(receiver, atLeastOnce()).applyTask(any(ReplicationTask.class));

        manager.shutdown();

        System.out.printf("✅ Mixed load completed successfully with %d total operations%n", total.get());
    }
}
