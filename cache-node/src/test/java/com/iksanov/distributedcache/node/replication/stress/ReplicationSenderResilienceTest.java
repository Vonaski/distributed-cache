package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.common.cluster.NodeInfo;
import com.iksanov.distributedcache.common.cluster.ReplicaManager;
import com.iksanov.distributedcache.node.replication.ReplicationSender;
import com.iksanov.distributedcache.node.replication.ReplicationTask;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Stress test for ReplicationSender resilience under simulated network latency and failures.
 * <p>
 * NOTE: this version avoids blocking sleeps in mocks to keep the test fast and deterministic.
 */
public class ReplicationSenderResilienceTest {

    private ReplicationSender sender;
    private ReplicaManager replicaManager;

    @AfterEach
    void cleanup() {
        if (sender != null) sender.shutdown();
    }

    @Test
    void shouldSurviveNetworkLatencyAndConnectionFailures_withCloneMocks_nonBlocking() throws Exception {
        replicaManager = mock(ReplicaManager.class);

        NodeInfo master = new NodeInfo("node-A", "127.0.0.1", 9000);
        NodeInfo replica1 = new NodeInfo("node-B", "127.0.0.1", 9001);
        NodeInfo replica2 = new NodeInfo("node-C", "127.0.0.1", 9002);
        when(replicaManager.getReplicas(master)).thenReturn(Set.of(replica1, replica2));

        Random random = new Random();

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

            when(clone.connect(anyString(), anyInt())).thenAnswer(invocation -> {
                ChannelFuture future = mock(ChannelFuture.class);
                boolean success = random.nextInt(10) > 2; // ~80% success
                when(future.isSuccess()).thenReturn(success);
                if (!success) when(future.cause()).thenReturn(new RuntimeException("simulated connect failed"));
                return future;
            });

            when(bootstrapMock.connect(anyString(), anyInt())).thenAnswer(invocation -> {
                ChannelFuture f = mock(ChannelFuture.class);
                when(f.isSuccess()).thenReturn(true);
                return f;
            });
        })) {
            sender = new ReplicationSender(replicaManager);

            int threads = 16;
            int operationsPerThread = 500;
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threads);
            AtomicInteger successCounter = new AtomicInteger(0);
            AtomicInteger failureCounter = new AtomicInteger(0);

            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int j = 0; j < operationsPerThread; j++) {
                            ReplicationTask task = ReplicationTask.ofSet("k-" + j, "v-" + j, "node-A");
                            try {
                                sender.replicate(master, task);
                                successCounter.incrementAndGet();
                            } catch (Exception e) {
                                failureCounter.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        fail("Worker error: " + e.getMessage());
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            long startNs = System.nanoTime();
            startLatch.countDown();
            boolean finished = doneLatch.await(120, TimeUnit.SECONDS);
            long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);

            executor.shutdownNow();

            assertTrue(finished, "Timeout waiting for workers to finish");

            System.out.printf("✅ Completed %d replicate() calls in %d ms%n", successCounter.get() + failureCounter.get(), durationMs);
            System.out.printf("Success: %d, Failures (simulated/exception): %d%n", successCounter.get(), failureCounter.get());

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
            assertTrue(connectObserved, "No connect() observed on clones or originals");
        }
    }
}
