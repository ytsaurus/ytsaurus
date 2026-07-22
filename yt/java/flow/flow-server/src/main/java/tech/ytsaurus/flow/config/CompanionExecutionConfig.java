package tech.ytsaurus.flow.config;

import java.time.Duration;
import java.util.Objects;

import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static tech.ytsaurus.flow.config.EnvironmentReader.ENV_VAR_FLOW_MODE;

/**
 * YT Flow companion execution configuration.
 * Mirrors {@code NYT::NFlow::NCompanion::TCompanionExecutionConfig}.
 * <p>
 * Environment contract for {@link #fromEnvironment(EnvironmentReader)}:
 * <ul>
 *   <li>{@code YT_FLOW_COMPANION_CONFIG} — YSON text, required.</li>
 *   <li>{@code YT_FLOW_MODE} — required, must be {@code Worker}.</li>
 * </ul>
 *
 * @param port           companion port.
 * @param monitoringPort companion monitoring HTTP port.
 * @param clusterUrl     YT proxy URL of the cluster running the pipeline.
 * @param pipelinePath   Cypress path of the pipeline this companion serves.
 * @param jobTtl         per-job TTL used by {@code JobContext} eviction.
 */
public record CompanionExecutionConfig(
        int port,
        int monitoringPort,
        String clusterUrl,
        String pipelinePath,
        Duration jobTtl
) implements YTreeConvertible {

    /**
     * Env var carrying the full companion config as YSON text.
     */
    public static final String ENV_VAR_COMPANION_CONFIG = "YT_FLOW_COMPANION_CONFIG";

    /**
     * Default job TTL when none is provided.
     */
    public static final Duration DEFAULT_JOB_TTL = Duration.ofMinutes(10);

    // YSON keys — must stay in sync with C++ TCompanionConfig::Register.
    private static final String KEY_PORT = "port";
    private static final String KEY_MONITORING_PORT = "monitoring_port";
    private static final String KEY_CLUSTER_URL = "cluster_url";
    private static final String KEY_PIPELINE_PATH = "pipeline_path";
    private static final String KEY_JOB_TTL_SECONDS = "job_ttl_seconds";

    public CompanionExecutionConfig {
        Objects.requireNonNull(clusterUrl, "clusterUrl");
        Objects.requireNonNull(pipelinePath, "pipelinePath");
        Objects.requireNonNull(jobTtl, "jobTtl");
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("port must be in [1, 65535], got " + port);
        }
        if (monitoringPort < 0 || monitoringPort > 65535) {
            throw new IllegalArgumentException(
                    "monitoringPort must be in [1, 65535], got " + monitoringPort);
        }
    }

    /**
     * Parse a config from its YSON text representation.
     */
    public static CompanionExecutionConfig fromYTreeText(String text) {
        return fromYTree(YTreeTextSerializer.deserialize(text));
    }

    /**
     * Create a {@code CompanionExecutionConfig} from the process environment
     * using a default {@link EnvironmentReader}.
     */
    public static CompanionExecutionConfig fromEnvironment() {
        return fromEnvironment(new EnvironmentReader());
    }

    /**
     * Create a {@code CompanionExecutionConfig} from a supplied {@link EnvironmentReader}.
     * <p> Reads {@code YT_FLOW_COMPANION_CONFIG} (YSON text) and {@code YT_FLOW_MODE}.
     */
    public static CompanionExecutionConfig fromEnvironment(EnvironmentReader envReader) {
        validateRunMode(envReader);
        return loadFromEnv(envReader);
    }

    /**
     * Build a config from a YTree map node. Missing fields use defaults.
     */
    public static CompanionExecutionConfig fromYTree(YTreeNode node) {
        if (!node.isMapNode()) {
            throw new IllegalArgumentException(
                    "CompanionExecutionConfig expects a YSON map, got: "
                            + node.getClass().getSimpleName());
        }
        var map = node.asMap();
        long jobTtlSeconds = YsonUtils.toLongOrDefault(map.get(KEY_JOB_TTL_SECONDS), DEFAULT_JOB_TTL.toSeconds());
        if (jobTtlSeconds < 0) {
            throw new IllegalArgumentException(
                    "%s must be non-negative, got %d".formatted(KEY_JOB_TTL_SECONDS, jobTtlSeconds)
            );
        }
        return new CompanionExecutionConfig(
                YsonUtils.toIntOrDefault(map.get(KEY_PORT), 0),
                YsonUtils.toIntOrDefault(map.get(KEY_MONITORING_PORT), 0),
                YsonUtils.toStringOrDefault(map.get(KEY_CLUSTER_URL), ""),
                YsonUtils.toStringOrDefault(map.get(KEY_PIPELINE_PATH), ""),
                Duration.ofSeconds(jobTtlSeconds)
        );
    }

    /**
     * Serialize this config into a YSON map node.
     */
    @Override
    public YTreeNode toYTree() {
        return YTree.mapBuilder()
                .key(KEY_PORT).value(port)
                .key(KEY_MONITORING_PORT).value(monitoringPort)
                .key(KEY_CLUSTER_URL).value(clusterUrl)
                .key(KEY_PIPELINE_PATH).value(pipelinePath)
                // Mirror C++ TCompanionConfig::JobTtlSeconds (plain Int64 seconds).
                .key(KEY_JOB_TTL_SECONDS).value(jobTtl.toSeconds())
                .endMap()
                .build();
    }

    /**
     * Serialize this config into YSON text (the form used in env vars).
     */
    public String toYTreeText() {
        return YTreeTextSerializer.serialize(toYTree());
    }

    /**
     * Return a copy with {@code port} replaced.
     */
    public CompanionExecutionConfig withPort(int newPort) {
        return new CompanionExecutionConfig(newPort, monitoringPort, clusterUrl, pipelinePath, jobTtl);
    }

    /**
     * Fluent builder for CompanionExecutionConfig.
     */
    public static Builder builder() {
        return new Builder();
    }

    private static void validateRunMode(EnvironmentReader envReader) {
        String raw = envReader.getVarOptional(ENV_VAR_FLOW_MODE)
                .orElseThrow(() -> new IllegalArgumentException(
                        "%s environment variable is not set".formatted(ENV_VAR_FLOW_MODE)));
        FlowRunMode runMode = FlowRunMode.valueOf(raw);
        if (runMode != FlowRunMode.Worker) {
            throw new IllegalArgumentException(
                    "Companion process started in non-worker mode: %s".formatted(runMode));
        }
    }

    private static CompanionExecutionConfig loadFromEnv(EnvironmentReader envReader) {
        return envReader.getVarOptional(ENV_VAR_COMPANION_CONFIG)
                .map(CompanionExecutionConfig::fromYTreeText)
                .orElseThrow(() -> new IllegalArgumentException(
                        "%s environment variable is not set".formatted(ENV_VAR_COMPANION_CONFIG)));
    }

    /**
     * Fluent builder for {@link CompanionExecutionConfig}.
     */
    public static final class Builder {
        private int port;
        private int monitoringPort;
        private String clusterUrl = "";
        private String pipelinePath = "";
        private Duration jobTtl = DEFAULT_JOB_TTL;

        private Builder() {
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder monitoringPort(int monitoringPort) {
            this.monitoringPort = monitoringPort;
            return this;
        }

        public Builder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public Builder pipelinePath(String pipelinePath) {
            this.pipelinePath = pipelinePath;
            return this;
        }

        public Builder jobTtl(Duration jobTtl) {
            this.jobTtl = jobTtl;
            return this;
        }

        public CompanionExecutionConfig build() {
            return new CompanionExecutionConfig(port, monitoringPort, clusterUrl, pipelinePath, jobTtl);
        }
    }
}
