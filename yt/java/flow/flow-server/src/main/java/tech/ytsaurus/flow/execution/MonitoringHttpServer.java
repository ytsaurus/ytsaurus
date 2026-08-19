package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimal embedded HTTP server used to expose monitoring endpoints
 * (notably {@code /metrics}) for the companion process.
 * <p>
 * Wraps a JDK {@link HttpServer} so that the rest of the codebase does not depend
 * on the {@code com.sun.net.httpserver} API directly.
 */
class MonitoringHttpServer {

    private static final Logger log = LoggerFactory.getLogger(MonitoringHttpServer.class);
    /**
     * Seconds to wait for in-flight requests to complete on stop. Solomon scrapes
     * are short, so a couple of seconds is enough to let an in-progress
     * {@code /metrics} response finish without aborting the connection.
     */
    private static final int STOP_DRAIN_TIMEOUT_SECONDS = 2;
    /** Poll interval while waiting for in-flight requests to drain on stop. */
    private static final long STOP_DRAIN_POLL_MILLIS = 20;
    private final int port;
    private final Map<String, HttpHandler> handlers = new HashMap<>();
    /**
     * Number of exchanges currently being served, used to drain in-flight requests on stop.
     * <p>
     * The count is bumped at dispatch time (when {@link HttpServer} hands an exchange to the
     * executor) rather than inside the handler, so a request that has been accepted but whose
     * handler task has not yet started running is still visible to the drain loop and is not
     * aborted by {@link #stop()}.
     * <p>
     * We track this ourselves because {@code HttpServer.stop(delay)} on JDK 17 always sleeps for
     * the whole {@code delay} when the server is idle — the early-return-when-idle behaviour was
     * only added in JDK 18. By counting in-flight requests we can stop immediately once they have
     * drained, restoring the JDK 18+ semantics.
     */
    private final AtomicInteger inFlightRequests = new AtomicInteger();
    private @Nullable HttpServer httpServer;
    private @Nullable ExecutorService executor;

    MonitoringHttpServer(int port, Map<String, HttpHandler> handlers) {
        this.port = port;
        for (Map.Entry<String, HttpHandler> entry : handlers.entrySet()) {
            log.info("Adding http handler for path {}", entry.getKey());
            addHandler(entry.getKey(), entry.getValue());
        }
    }

    private void addHandler(String path, HttpHandler handler) {
        if (handlers.containsKey(path)) {
            throw new IllegalArgumentException("Handler for path " + path + " already exists");
        }
        handlers.put(path, handler);
    }

    void start() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        executor = Executors.newCachedThreadPool();
        httpServer.setExecutor(drainTrackingExecutor(executor));
        handlers.forEach(httpServer::createContext);
        httpServer.start();
    }

    /**
     * Wraps {@code delegate} so every dispatched exchange is counted in {@link #inFlightRequests}
     * for the whole time it is being served. {@link HttpServer} invokes {@code execute} synchronously
     * the moment it starts serving a request, so the count rises before the handler task is scheduled
     * on a worker thread — closing the window between accept and handler start.
     */
    private Executor drainTrackingExecutor(Executor delegate) {
        return command -> {
            inFlightRequests.incrementAndGet();
            try {
                delegate.execute(() -> {
                    try {
                        command.run();
                    } finally {
                        inFlightRequests.decrementAndGet();
                    }
                });
            } catch (RejectedExecutionException e) {
                // Task never ran, so undo the increment to keep the counter honest.
                inFlightRequests.decrementAndGet();
                throw e;
            }
        };
    }

    void stop() {
        if (httpServer == null) {
            return;
        }
        // Let an in-progress scrape finish, but wait no longer than the drain timeout. Then stop
        // immediately (delay 0) so an idle server shuts down without blocking for the whole timeout.
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(STOP_DRAIN_TIMEOUT_SECONDS);
        while (inFlightRequests.get() > 0 && System.nanoTime() < deadlineNanos) {
            try {
                Thread.sleep(STOP_DRAIN_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        httpServer.stop(0);
        httpServer = null;
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    int getPort() {
        return httpServer != null ? httpServer.getAddress().getPort() : -1;
    }
}
