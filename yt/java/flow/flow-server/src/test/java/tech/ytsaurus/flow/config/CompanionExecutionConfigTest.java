package tech.ytsaurus.flow.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompanionExecutionConfigTest {

    private static EnvironmentReader workerEnv(String companionConfigYson) {
        EnvironmentReader reader = Mockito.mock(EnvironmentReader.class);
        Mockito.when(reader.getVarOptional(Mockito.anyString())).thenReturn(Optional.empty());
        Mockito.when(reader.getVarOptional(EnvironmentReader.ENV_VAR_FLOW_MODE))
                .thenReturn(Optional.of("Worker"));
        Mockito.when(reader.getVarOptional(CompanionExecutionConfig.ENV_VAR_COMPANION_CONFIG))
                .thenReturn(Optional.of(companionConfigYson));
        return reader;
    }

    @Test
    public void parsesAllFieldsFromCompanionConfigYson() throws IOException {
        String yson = loadResource("/companion_config.yson");

        CompanionExecutionConfig config = CompanionExecutionConfig.fromYTreeText(yson);

        assertEquals(8080, config.port());
        assertEquals(8081, config.monitoringPort());
        assertEquals("test_cluster", config.clusterUrl());
        assertEquals("//some/yt/path/pipeline", config.pipelinePath());
        assertEquals(Duration.ofHours(1), config.jobTtl());
    }

    @Test
    public void companionConfigYsonRoundTripsThroughSerializer() {
        CompanionExecutionConfig original = CompanionExecutionConfig.builder()
                .port(12345)
                .monitoringPort(54321)
                .clusterUrl("cluster-x")
                .pipelinePath("//x/y/z")
                .jobTtl(Duration.ofMinutes(7))
                .build();

        CompanionExecutionConfig parsed = CompanionExecutionConfig.fromYTreeText(original.toYTreeText());

        assertEquals(original, parsed);
    }

    @Test
    public void companionConfigUsesDefaultsForMissingFields() {
        CompanionExecutionConfig config = CompanionExecutionConfig.fromYTreeText("{\"port\" = 9999}");

        assertEquals(9999, config.port());
        assertEquals(0, config.monitoringPort());
        assertEquals("", config.clusterUrl());
        assertEquals("", config.pipelinePath());
        assertEquals(CompanionExecutionConfig.DEFAULT_JOB_TTL, config.jobTtl());
    }

    @Test
    public void companionConfigRejectsNonMapYson() {
        assertThrows(IllegalArgumentException.class,
                () -> CompanionExecutionConfig.fromYTreeText("[1; 2; 3]"));
    }

    @Test
    public void fromEnvironmentExposesAllCompanionConfigFields() throws IOException {
        String yson = loadResource("/companion_config.yson");
        EnvironmentReader reader = workerEnv(yson);

        CompanionExecutionConfig config = CompanionExecutionConfig.fromEnvironment(reader);

        assertEquals(8080, config.port());
        assertEquals(Duration.ofHours(1), config.jobTtl());
        assertEquals("test_cluster", config.clusterUrl());
        assertEquals("//some/yt/path/pipeline", config.pipelinePath());
    }

    @Test
    public void jobTtlComesFromCompanionConfigYson() {
        CompanionExecutionConfig source = CompanionExecutionConfig.builder()
                .port(1234)
                .clusterUrl("c")
                .pipelinePath("//p")
                .jobTtl(Duration.ofMinutes(5))
                .build();
        EnvironmentReader reader = workerEnv(source.toYTreeText());

        CompanionExecutionConfig config = CompanionExecutionConfig.fromEnvironment(reader);

        assertEquals(Duration.ofMinutes(5), config.jobTtl());
    }

    @Test
    public void exceptionOnMissingCompanionConfigEnvVar() {
        EnvironmentReader reader = Mockito.mock(EnvironmentReader.class);
        Mockito.when(reader.getVarOptional(Mockito.anyString())).thenReturn(Optional.empty());
        Mockito.when(reader.getVarOptional(EnvironmentReader.ENV_VAR_FLOW_MODE))
                .thenReturn(Optional.of("Worker"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> CompanionExecutionConfig.fromEnvironment(reader));
        assertTrue(
                ex.getMessage().contains(CompanionExecutionConfig.ENV_VAR_COMPANION_CONFIG),
                "Exception message should name the missing env var");
    }

    @Test
    public void exceptionOnWrongFlowMode() {
        EnvironmentReader reader = Mockito.mock(EnvironmentReader.class);
        Mockito.when(reader.getVarOptional(Mockito.anyString())).thenReturn(Optional.empty());
        Mockito.when(reader.getVarOptional(EnvironmentReader.ENV_VAR_FLOW_MODE))
                .thenReturn(Optional.of("Controller"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> CompanionExecutionConfig.fromEnvironment(reader));
        assertTrue(
                ex.getMessage().contains("non-worker"),
                "Exception message should mention the wrong-mode reason, got: " + ex.getMessage());
    }

    @Test
    public void exceptionOnMissingFlowMode() {
        EnvironmentReader reader = Mockito.mock(EnvironmentReader.class);
        Mockito.when(reader.getVarOptional(Mockito.anyString())).thenReturn(Optional.empty());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> CompanionExecutionConfig.fromEnvironment(reader));
        assertTrue(
                ex.getMessage().contains(EnvironmentReader.ENV_VAR_FLOW_MODE),
                "Exception message should name the missing env var, got: " + ex.getMessage());
    }

    @Test
    public void withPortReturnsCopyWithReplacedPort() {
        CompanionExecutionConfig original = CompanionExecutionConfig.builder()
                .port(1000)
                .monitoringPort(2000)
                .clusterUrl("c")
                .pipelinePath("p")
                .jobTtl(Duration.ofMinutes(3))
                .build();

        CompanionExecutionConfig patched = original.withPort(5000);

        assertEquals(1000, original.port(), "original must be unchanged");
        assertEquals(5000, patched.port());
        // Other fields preserved.
        assertEquals(2000, patched.monitoringPort());
        assertEquals("c", patched.clusterUrl());
        assertEquals("p", patched.pipelinePath());
        assertEquals(Duration.ofMinutes(3), patched.jobTtl());
    }

    // job_ttl_seconds parsing.

    @Test
    public void jobTtlParsesIntegerSeconds() {
        // Wire format: plain Int64 seconds (matches C++ TCompanionConfig::JobTtlSeconds).
        CompanionExecutionConfig config = CompanionExecutionConfig.fromYTreeText("{\"job_ttl_seconds\" = 90;}");

        assertEquals(Duration.ofSeconds(90), config.jobTtl());
    }

    @Test
    public void jobTtlRejectsNegativeInteger() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> CompanionExecutionConfig.fromYTreeText("{\"job_ttl_seconds\" = -1;}"));
        assertTrue(
                ex.getMessage().contains("non-negative"),
                "Exception message should mention non-negativity, got: " + ex.getMessage());
    }

    @Test
    public void jobTtlRoundTripsThroughYson() {
        // Java-produced YSON must be consumable by the C++ side, which expects
        // an Int64 number of seconds. Round-trip through the serializer.
        CompanionExecutionConfig config = CompanionExecutionConfig.builder()
                .jobTtl(Duration.ofSeconds(42))
                .build();

        CompanionExecutionConfig parsed = CompanionExecutionConfig.fromYTreeText(config.toYTreeText());

        assertEquals(Duration.ofSeconds(42), parsed.jobTtl());
    }

    private static String loadResource(String path) throws IOException {
        try (InputStream in = CompanionExecutionConfigTest.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IOException("Resource not found: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
