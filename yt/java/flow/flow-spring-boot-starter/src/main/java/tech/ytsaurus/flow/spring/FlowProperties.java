package tech.ytsaurus.flow.spring;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Spring properties for the Flow companion server.
 * <p>
 * The authoritative runtime config is supplied by the C++ worker through
 * {@code YT_FLOW_COMPANION_CONFIG}. {@code flow.server.port}, when set, is a
 * <b>dev-only</b> override of the port field; other fields stay untouched.
 *
 * <pre>
 * flow:
 *   server:
 *     port: 8080
 * </pre>
 *
 * @see FlowAutoConfiguration
 */
@ConfigurationProperties(prefix = "flow")
public class FlowProperties {

    private Server server = new Server();

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    /**
     * Server configuration properties for the gRPC companion server.
     */
    public static class Server {

        /**
         * Minimum valid port number.
         */
        public static final int MIN_PORT = 1;

        /**
         * Maximum valid port number.
         */
        public static final int MAX_PORT = 65535;

        /**
         * Dev-only override of the {@code port} field from
         * {@code YT_FLOW_COMPANION_CONFIG}. Range: 1–65535.
         */
        private @Nullable Integer port;

        public @Nullable Integer getPort() {
            return port;
        }

        /**
         * Sets the server port.
         *
         * @param port the port number (1-65535), or null to use environment variable
         * @throws IllegalArgumentException if port is outside valid range
         */
        public void setPort(@Nullable Integer port) {
            if (port != null && (port < MIN_PORT || port > MAX_PORT)) {
                throw new IllegalArgumentException(
                        "Port must be between " + MIN_PORT + " and " + MAX_PORT + ", got: " + port);
            }
            this.port = port;
        }

        /**
         * Validates that the port is within the valid range.
         *
         * @return true if port is null or within valid range
         */
        public boolean isValid() {
            return port == null || (port >= MIN_PORT && port <= MAX_PORT);
        }
    }
}
