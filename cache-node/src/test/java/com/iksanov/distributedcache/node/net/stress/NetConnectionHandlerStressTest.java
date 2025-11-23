package com.iksanov.distributedcache.node.net.stress;

import com.iksanov.distributedcache.common.dto.CacheRequest;
import com.iksanov.distributedcache.common.dto.CacheResponse;
import com.iksanov.distributedcache.node.core.CacheStore;
import com.iksanov.distributedcache.node.metrics.NetMetrics;
import com.iksanov.distributedcache.node.net.NetConnectionHandler;
import com.iksanov.distributedcache.node.net.RequestProcessor;
import com.iksanov.distributedcache.node.replication.ReplicationManager;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Stress test for {@link NetConnectionHandler}.
 * <p>
 * Simulates a large number of concurrent inbound {@link CacheRequest}s across multiple threads,
 * each handled by its own {@link EmbeddedChannel} pipeline.
 * <p>
 * This test validates:
 * <ul>
 *     <li>All requests are processed successfully without race conditions</li>
 *     <li>No unexpected exceptions or hangs occur under load</li>
 *     <li>The total number of outbound {@link CacheResponse}s matches inbound requests</li>
 * </ul>
 */
class NetConnectionHandlerStressTest {

    private static final int THREADS = 8;
    private static final int REQUEST_COUNT = 10_000;
    private static final int REQUESTS_PER_THREAD = REQUEST_COUNT / THREADS;

    @Test
    @DisplayName("Should process 10k concurrent requests across multiple channels")
    void shouldHandleConcurrentRequests() throws Exception {
        CacheStore store = mock(CacheStore.class);
        NetMetrics metrics = mock(NetMetrics.class);
        RequestProcessor processor = mock(RequestProcessor.class);
        ReplicationManager replicationManager = mock(ReplicationManager.class);
        when(store.get(anyString())).thenReturn("v");
        doNothing().when(store).put(anyString(), anyString());
        doNothing().when(store).delete(anyString());
        ExecutorService pool = Executors.newWorkStealingPool(THREADS);
        CountDownLatch latch = new CountDownLatch(THREADS);
        List<Future<Integer>> results = new ArrayList<>();
        long start = System.nanoTime();

        for (int t = 0; t < THREADS; t++) {
            results.add(pool.submit(() -> {
                var handler = new NetConnectionHandler(processor, metrics);
                var channel = new EmbeddedChannel(handler);

                int responses = 0;
                try {
                    for (int i = 0; i < REQUESTS_PER_THREAD; i++) {
                        CacheRequest req = new CacheRequest(
                                "req-" + Thread.currentThread().getId() + "-" + i,
                                System.currentTimeMillis(),
                                CacheRequest.Command.GET,
                                "key-" + i,
                                null
                        );
                        channel.writeInbound(req);
                    }
                    channel.finish();
                    Object msg;
                    while ((msg = channel.readOutbound()) != null) {
                        assertInstanceOf(CacheResponse.class, msg);
                        responses++;
                    }
                } finally {
                    latch.countDown();
                    channel.finishAndReleaseAll();
                }
                return responses;
            }));
        }

        long timeoutSec = Math.max(30, REQUEST_COUNT / 300);
        boolean finished = latch.await(timeoutSec, TimeUnit.SECONDS);
        pool.shutdown();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        int totalResponses = 0;
        for (Future<Integer> f : results) {
            totalResponses += f.get();
        }
        System.out.printf("Processed %d requests across %d threads in %d ms%n", REQUEST_COUNT, THREADS, elapsedMs);
        assertTrue(finished, "All threads must complete within timeout (" + timeoutSec + "s)");
        assertEquals(REQUEST_COUNT, totalResponses, "Total outbound responses must equal inbound requests");
        double throughput = REQUEST_COUNT / (elapsedMs / 1000.0);
        System.out.printf("Throughput: %.2f req/s%n", throughput);
    }
}
