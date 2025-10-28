package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.replication.ReplicationSender;
import com.iksanov.distributedcache.node.replication.ReplicationTask;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.bootstrap.Bootstrap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Stress test for ReplicationSender that properly handles bootstrap.clone() behavior.
 */
public class ReplicationSenderStressTest {

    private ReplicationSender sender;
    private ReplicaManager replicaManager;

    @AfterEach
    void cleanup() {
        if (sender != null) {
            sender.shutdown();
            sender = null;
        }
    }

    @Test
    void shouldHandleMassiveParallelReplicationCalls_withCloneMocks() throws Exception {
        replicaManager = mock(ReplicaManager.class);
        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica1 = new NodeInfo("node-B", "127.0.0.1", 9001);
        NodeInfo replica2 = new NodeInfo("node-C", "127.0.0.1", 9002);
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replica1, replica2));

        List<Bootstrap> cloneList = new CopyOnWriteArrayList<>();

        try (MockedConstruction<Bootstrap> mocked = mockConstruction(Bootstrap.class, (bootstrapMock, context) -> {
            when(bootstrapMock.group(any(EventLoopGroup.class))).thenReturn(bootstrapMock);
            when(bootstrapMock.channel(any())).thenReturn(bootstrapMock);
            when(bootstrapMock.option(any(), any())).thenReturn(bootstrapMock);
            when(bootstrapMock.handler(any())).thenReturn(bootstrapMock);

            Bootstrap clone = mock(Bootstrap.class);
            cloneList.add(clone);

            when(bootstrapMock.clone()).thenReturn(clone);

            when(clone.group(any(EventLoopGroup.class))).thenReturn(clone);
            when(clone.channel(any())).thenReturn(clone);
            when(clone.option(any(), any())).thenReturn(clone);
            when(clone.handler(any())).thenReturn(clone);
            when(clone.clone()).thenReturn(clone);

            ChannelFuture future = mock(ChannelFuture.class);
            when(future.isSuccess()).thenReturn(true);

            when(bootstrapMock.connect(anyString(), anyInt())).thenReturn(future);
            when(clone.connect(anyString(), anyInt())).thenReturn(future);
        })) {

            sender = new ReplicationSender(replicaManager);

            int threads = 20;
            int operationsPerThread = 500; // total 10k replicate() calls
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threads);
            AtomicInteger counter = new AtomicInteger(0);

            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < operationsPerThread; j++) {
                            ReplicationTask task = ReplicationTask.ofSet("k-" + j, "v-" + j, "node-A");
                            sender.replicate(master, task);
                            counter.incrementAndGet();
                        }
                    } catch (Exception e) {
                        fail("Worker error: " + e.getMessage());
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            long start = System.nanoTime();
            startLatch.countDown();
            assertTrue(doneLatch.await(30, TimeUnit.SECONDS), "Timeout waiting for threads to finish");
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

            executor.shutdownNow();

            System.out.printf("✅ Completed %d replicate() calls in %d ms%n", counter.get(), elapsedMs);

            boolean connectObserved = false;
            for (Bootstrap clone : cloneList) {
                try {
                    verify(clone, atLeastOnce()).connect(anyString(), anyInt());
                    connectObserved = true;
                    break;
                } catch (Throwable ignored) {
                }
            }
            for (Bootstrap constructed : mocked.constructed()) {
                try {
                    verify(constructed, atLeastOnce()).connect(anyString(), anyInt());
                    connectObserved = true;
                    break;
                } catch (Throwable ignored) {
                }
            }
            assertTrue(connectObserved, "No connect() was observed on bootstrap clones or originals");
        }
    }
}
