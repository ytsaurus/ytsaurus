package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.sun.net.httpserver.HttpHandler;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MonitoringHttpServer}, focused on the graceful-drain contract of {@link
 * MonitoringHttpServer#stop()}: it must return promptly when idle, let an in-flight scrape finish,
 * and still terminate within the drain timeout when a request never completes.
 */
class MonitoringHttpServerTest {

    private static HttpHandler respond(String body) {
        return exchange -> {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        };
    }

    @Test
    void stopIsFastWhenIdle() throws IOException {
        MonitoringHttpServer server = new MonitoringHttpServer(0, Map.of("/metrics", respond("ok")));
        server.start();
        try {
            long startNanos = System.nanoTime();
            server.stop();
            long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;
            assertTrue(elapsedMillis < 1_000,
                    "stop() on an idle server should return promptly, took " + elapsedMillis + " ms");
        } finally {
            server.stop();  // idempotent
        }
    }

    @Test
    void stopDrainsInFlightRequestWithoutAborting() throws Exception {
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch releaseHandler = new CountDownLatch(1);
        HttpHandler slow = exchange -> {
            handlerStarted.countDown();
            awaitQuietly(releaseHandler);
            byte[] bytes = "done".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        };

        MonitoringHttpServer server = new MonitoringHttpServer(0, Map.of("/slow", slow));
        server.start();
        int port = server.getPort();

        HttpClient client = HttpClient.newHttpClient();
        CompletableFuture<HttpResponse<String>> response = client.sendAsync(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/slow"))
                        .timeout(Duration.ofSeconds(5))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        // Wait until the request is actually being served before stopping.
        assertTrue(handlerStarted.await(5, TimeUnit.SECONDS), "handler should have started");

        AtomicBoolean stopReturned = new AtomicBoolean(false);
        Thread stopper = new Thread(() -> {
            server.stop();
            stopReturned.set(true);
        });
        stopper.start();

        // stop() must still be draining while the handler is blocked.
        Thread.sleep(200);
        assertFalse(stopReturned.get(), "stop() must wait for the in-flight request to finish");

        // Let the handler finish; stop() should then return and the client should get the full response.
        releaseHandler.countDown();
        stopper.join(TimeUnit.SECONDS.toMillis(5));
        assertTrue(stopReturned.get(), "stop() should return once the request drains");

        HttpResponse<String> result = response.get(5, TimeUnit.SECONDS);
        assertEquals(200, result.statusCode(), "in-flight scrape must not be aborted by stop()");
        assertEquals("done", result.body());
    }

    @Test
    void stopReturnsWithinTimeoutWhenRequestNeverCompletes() throws Exception {
        CountDownLatch handlerStarted = new CountDownLatch(1);
        CountDownLatch neverReleased = new CountDownLatch(1);
        HttpHandler stuck = exchange -> {
            handlerStarted.countDown();
            awaitQuietly(neverReleased);  // released only via executor.shutdownNow() interrupt on stop
        };

        MonitoringHttpServer server = new MonitoringHttpServer(0, Map.of("/stuck", stuck));
        server.start();
        int port = server.getPort();

        HttpClient client = HttpClient.newHttpClient();
        client.sendAsync(
                HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/stuck"))
                        .timeout(Duration.ofSeconds(10))
                        .build(),
                HttpResponse.BodyHandlers.ofString());

        assertTrue(handlerStarted.await(5, TimeUnit.SECONDS), "handler should have started");

        long startNanos = System.nanoTime();
        server.stop();
        long elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000;

        // The drain timeout is 2s: stop() should wait roughly that long, then abort — never hang.
        assertTrue(elapsedMillis >= 1_500,
                "stop() should wait for the drain timeout before aborting, took " + elapsedMillis + " ms");
        assertTrue(elapsedMillis < 5_000,
                "stop() must return shortly after the drain timeout, took " + elapsedMillis + " ms");
    }

    @Test
    void getPortReflectsRunningState() throws IOException {
        MonitoringHttpServer server = new MonitoringHttpServer(0, Map.of("/metrics", respond("ok")));
        assertEquals(-1, server.getPort(), "getPort() should be -1 before start");
        server.start();
        assertTrue(server.getPort() > 0, "getPort() should return the assigned port while running");
        server.stop();
        assertEquals(-1, server.getPort(), "getPort() should be -1 after stop");
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
