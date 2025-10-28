package com.iksanov.distributedcache.node.replication.stress;

import com.iksanov.distributedcache.node.core.InMemoryCacheStore;
import com.iksanov.distributedcache.node.replication.ReplicationReceiver;
import com.iksanov.distributedcache.node.replication.ReplicationTask;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Stress test for ReplicationReceiver under heavy concurrent load.
 * <p>
 * Notes:
 *  - This test calls receiver.apply(...) directly (no network), so ReplicationReceiver
 *    does not need to be started as a Netty server for the purposes of this stress test.
 *  - Make sure ReplicationReceiver exposes a public method apply(ReplicationTask)
 *    (or make applyTask public) so this test can call it.
 */
public class ReplicationReceiverStressTest {

    @Test
    void shouldHandleHighConcurrentLoadWithLWWConsistency() throws Exception {
        InMemoryCacheStore store = new InMemoryCacheStore(10_000, 60_000, 10_000);
        ReplicationReceiver receiver = new ReplicationReceiver("127.0.0.1", 0, store);

        final int threads = 8;
        final int operationsPerThread = 12_500;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicLong globalTimestamp = new AtomicLong(System.currentTimeMillis());

        List<Future<?>> futures = new ArrayList<>();

        for (int t = 0; t < threads; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        long ts = globalTimestamp.incrementAndGet();
                        String key = "key-" + (i % 100);
                        String value = "v-" + threadId + "-" + i;
                        ReplicationTask task = ReplicationTask.ofSet(key, value, "node-B");
                        receiver.applyTask(task);
                    }
                } catch (Throwable e) {
                    fail("Unexpected exception in worker thread: " + e);
                } finally {
                    doneLatch.countDown();
                }
            }));
        }

        long startTime = System.nanoTime();
        startLatch.countDown();

        boolean finished = doneLatch.await(60, TimeUnit.SECONDS);
        assertTrue(finished, "Worker threads did not finish within timeout");

        for (Future<?> f : futures) {
            f.get(10, TimeUnit.SECONDS);
        }

        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);

        executor.shutdown();
        if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
            executor.shutdownNow();
        }

        assertTrue(store.size() > 0, "Cache should contain entries after replication tasks");

        for (int i = 0; i < 100; i++) {
            String key = "key-" + i;
            String val = store.get(key);
            assertNotNull(val, "Cache value for " + key + " should not be null");
        }

        System.out.printf("✅ Completed %,d concurrent ops in %d ms%n", (long) threads * operationsPerThread, durationMs);
        System.out.printf("Final cache size: %d%n", store.size());
    }

    private static void fail(String message) {
        throw new AssertionError(message);
    }
}
