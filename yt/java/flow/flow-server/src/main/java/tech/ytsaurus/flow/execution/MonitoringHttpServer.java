package tech.ytsaurus.flow.execution;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

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
    private final int port;
    private final Map<String, HttpHandler> handlers = new HashMap<>();
    private @Nullable HttpServer httpServer;

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
        httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        handlers.forEach(httpServer::createContext);
        httpServer.start();
    }

    void stop() {
        if (httpServer != null) {
            httpServer.stop(STOP_DRAIN_TIMEOUT_SECONDS);
            httpServer = null;
        }
    }

    int getPort() {
        return httpServer != null ? httpServer.getAddress().getPort() : -1;
    }
}
