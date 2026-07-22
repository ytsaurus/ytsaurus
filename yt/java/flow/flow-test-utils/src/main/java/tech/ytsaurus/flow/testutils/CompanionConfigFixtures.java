package tech.ytsaurus.flow.testutils;

import java.time.Duration;

import tech.ytsaurus.flow.config.CompanionExecutionConfig;

/**
 * Test fixtures for {@link CompanionExecutionConfig}.
 */
public final class CompanionConfigFixtures {

    /**
     * Ephemeral port — asks the OS to assign one.
     */
    public static final int EPHEMERAL_PORT = 0;

    private CompanionConfigFixtures() {
    }

    /**
     * A complete {@link CompanionExecutionConfig} with deterministic dummy values.
     */
    public static CompanionExecutionConfig defaults() {
        return CompanionExecutionConfig.builder()
                .port(EPHEMERAL_PORT)
                .monitoringPort(EPHEMERAL_PORT)
                .clusterUrl("test-cluster")
                .pipelinePath("//test/pipeline")
                .jobTtl(Duration.ofMinutes(10))
                .build();
    }

    /**
     * {@link #defaults()} with {@code port} overridden.
     */
    public static CompanionExecutionConfig withPort(int port) {
        return defaults().withPort(port);
    }
}
