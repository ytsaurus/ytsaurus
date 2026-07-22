package tech.ytsaurus.flow.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelineRunnerConfigTest {

    private String pipelinePath;

    @BeforeEach
    void init() {
        this.pipelinePath = Objects.requireNonNull(getClass().getClassLoader().getResource("config.yson")).getFile();
    }

    @Test
    public void testLoadFile() {
        var config = new PipelineRunnerConfig(pipelinePath);
        assertEquals("test_cluster", config.getClusterUrl());
        assertEquals("test_cluster", config.getClusterAlias());
        assertEquals("//some/yt/path/pipeline", config.getPipelinePath());
        assertEquals(Optional.of("test"), config.getProxyRole());
        assertNull(config.getRunMode());
    }

    @Test
    public void testProxyRoleMissing(@TempDir Path tempDir) throws IOException {
        Path specFile = writeSpec(tempDir, """
                {
                    "cluster_url" = "test_cluster";
                    "path" = "//some/yt/path/pipeline";
                }
                """);
        var config = new PipelineRunnerConfig(specFile.toString());
        assertTrue(config.getProxyRole().isEmpty());
    }

    @Test
    public void testProxyRoleEntity(@TempDir Path tempDir) throws IOException {
        Path specFile = writeSpec(tempDir, """
                {
                    "cluster_url" = "test_cluster";
                    "path" = "//some/yt/path/pipeline";
                    "proxy_role" = #;
                }
                """);
        var config = new PipelineRunnerConfig(specFile.toString());
        assertTrue(config.getProxyRole().isEmpty());
    }

    @Test
    public void testClusterAliasResolution() {
        YTreeNode clusterAliasConfig = YTree.builder()
                .beginMap()
                .key("test_cluster")
                .value("localhost:12345")
                .endMap()
                .build();
        EnvironmentReader reader = Mockito.mock(EnvironmentReader.class);
        Mockito.when(reader.getVarOptional(EnvironmentReader.ENV_VAR_YT_PROXY_URL_ALIASING_CONFIG))
                .thenReturn(Optional.of(clusterAliasConfig.toString()));
        var config = new PipelineRunnerConfig(pipelinePath, reader);
        assertEquals("localhost:12345", config.getClusterUrl());
        assertEquals("test_cluster", config.getClusterAlias());
    }

    @Test
    public void testConfigPatch() {
        YTreeNode configPatch = YTree.builder()
                .beginMap()
                .key("proxy_role")
                .value("test_patched")
                .endMap()
                .build();
        EnvironmentReader reader = Mockito.mock(EnvironmentReader.class);
        Mockito.when(reader.getVarOptional(EnvironmentReader.ENV_VAR_YT_FLOW_CONFIG))
                .thenReturn(Optional.of(configPatch.toString()));
        var config = new PipelineRunnerConfig(pipelinePath, reader);
        assertEquals(Optional.of("test_patched"), config.getProxyRole());
    }

    @Test
    public void testFailedValidation() {
        EnvironmentReader reader = Mockito.mock(EnvironmentReader.class);
        Mockito.when(reader.getVarOptional(EnvironmentReader.ENV_VAR_FLOW_MODE))
                .thenReturn(Optional.of("Controller"));
        assertThrows(
                IllegalArgumentException.class,
                () -> new PipelineRunnerConfig(pipelinePath, reader),
                "Controller mode is not supported yet"
        );
    }

    private static Path writeSpec(Path dir, String yson) throws IOException {
        Path specFile = dir.resolve("spec.yson");
        Files.writeString(specFile, yson);
        return specFile;
    }
}
