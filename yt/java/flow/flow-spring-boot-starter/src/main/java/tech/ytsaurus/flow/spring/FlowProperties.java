package tech.ytsaurus.flow.spring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    /**
     * Full name of the {@link #getRunMode() run mode} property.
     */
    public static final String RUN_MODE_PROPERTY = "flow.run-mode";

    /**
     * Value of {@link #RUN_MODE_PROPERTY} that selects the runner, mirroring an unset
     * {@code YT_FLOW_MODE}.
     */
    public static final String RUNNER_MODE = "runner";

    /**
     * Full name of the {@code runner.enabled} property.
     */
    public static final String RUNNER_ENABLED_PROPERTY = "flow.runner.enabled";

    private Server server = new Server();

    private Runner runner = new Runner();

    public Runner getRunner() {
        return runner;
    }

    public void setRunner(Runner runner) {
        this.runner = runner;
    }

    private List<String> entityScanPackages = new ArrayList<>();

    private @Nullable String runMode;

    /**
     * Overrides {@code YT_FLOW_MODE} for this application: {@code Worker} configures the companion
     * beans, {@code runner} the launch. Normally unset — the worker exports {@code YT_FLOW_MODE} to
     * the companion it spawns, and its absence means the runner. Intended for tests, which cannot
     * set an environment variable in their own JVM.
     *
     * @return the configured run mode override, or {@code null} when the environment decides.
     */
    public @Nullable String getRunMode() {
        return runMode;
    }

    public void setRunMode(@Nullable String runMode) {
        this.runMode = runMode;
    }

    public Server getServer() {
        return server;
    }

    public void setServer(Server server) {
        this.server = server;
    }

    /**
     * Extra packages to scan for {@link tech.ytsaurus.flow.row.FlowMessage} POJOs, in addition to the
     * Spring Boot auto-configuration packages. Empty (the default) scans only the latter.
     *
     * @return an unmodifiable view of the configured additional scan packages (never {@code null}).
     */
    public List<String> getEntityScanPackages() {
        return Collections.unmodifiableList(entityScanPackages);
    }

    public void setEntityScanPackages(List<String> entityScanPackages) {
        this.entityScanPackages = new ArrayList<>(entityScanPackages);
    }

    /**
     * Properties of the pipeline launch in runner mode.
     */
    public static class Runner {

        /**
         * Whether {@code FlowRunnerBootstrap} may launch the pipeline. Set to {@code false} in a
         * test whose framework the built-in test detection does not recognize.
         */
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
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
