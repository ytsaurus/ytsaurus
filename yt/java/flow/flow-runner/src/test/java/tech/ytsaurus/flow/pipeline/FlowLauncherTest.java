package tech.ytsaurus.flow.pipeline;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tech.ytsaurus.flow.testutils.MockEnvironmentReader;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowLauncherTest {

    // Mirrors yt-porto-layers.yson for the launcher's runtime major version.
    private static final String EXPECTED_JDK_LAYER =
            "//porto_layers/delta/jdk/jdk21/layer_with_jdk21_latest.tar.gz";
    private static final String EXPECTED_JAVA_BIN_PATH = "/opt/jdk21/bin/java";
    private static final String EXPECTED_SYSTEM_LAYER =
            "//porto_layers/base/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz";

    private YTreeNode config;
    private MockEnvironmentReader env;
    private FlowLauncher launcher;

    @BeforeEach
    void init() {
        String pipelinePath = Objects.requireNonNull(
                getClass().getClassLoader().getResource("vanilla_pipeline.yson")).getFile();
        config = loadConfig(pipelinePath);
        env = new MockEnvironmentReader();
        launcher = new FlowLauncher(env) {
            @Override
            protected List<Path> discoverCompanionJars() {
                return List.of(Path.of("/build/lib/flow-runner.jar"), Path.of("/build/lib/flow-core.jar"));
            }
        };
    }

    private YTreeNode loadConfig(String path) {
        try {
            YsonParser parser = new YsonParser(Files.readAllBytes(Path.of(path)));
            YTreeBuilder builder = YTree.builder();
            parser.parseNode(builder);
            return builder.build();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Drives the launcher end-to-end against the parsed test pipeline. */
    private void enrich() {
        YTreeMapNode root = config.mapNode();
        launcher.enrichVanilla(root.getOrThrow("vanilla").mapNode());
        launcher.patchCompanionResources(root.getOrThrow("spec").mapNode());
    }

    private YTreeMapNode worker() {
        return config.mapNode().getOrThrow("vanilla").mapNode().getOrThrow("worker").mapNode();
    }

    private YTreeMapNode controller() {
        return config.mapNode().getOrThrow("vanilla").mapNode().getOrThrow("controller").mapNode();
    }

    private YTreeMapNode companionResource() {
        return config.mapNode()
                .getOrThrow("spec").mapNode()
                .getOrThrow("resources").mapNode()
                .getOrThrow("CompanionManager").mapNode();
    }

    private YTreeMapNode companionParameters() {
        return companionResource().getOrThrow("parameters").mapNode();
    }

    @Test
    void testShipsCompanionJarsAsCleanGlob() {
        enrich();

        Map<String, YTreeNode> localFiles = worker().getOrThrow("local_files").asMap();
        assertEquals(2, localFiles.size());
        assertEquals(
                "/build/lib/flow-runner.jar",
                localFiles.get(FlowLauncher.COMPANION_JARS_DIR + "/flow-runner.jar").stringValue());
        assertEquals(
                "/build/lib/flow-core.jar",
                localFiles.get(FlowLauncher.COMPANION_JARS_DIR + "/flow-core.jar").stringValue());
    }

    @Test
    void testAppliesPortoLayersFromConfigToBothTasks() {
        enrich();

        for (YTreeMapNode task : List.of(controller(), worker())) {
            List<String> layers = task.getOrThrow("layers").asList().stream()
                    .map(YTreeNode::stringValue)
                    .toList();
            assertEquals(List.of(EXPECTED_JDK_LAYER), layers);
            assertEquals(EXPECTED_SYSTEM_LAYER, task.getOrThrow("system_layer_path").stringValue());
        }
    }

    @Test
    void testRewritesResourceIntoGenericCompanionManager() {
        enrich();

        YTreeMapNode resource = companionResource();
        assertEquals(
                "NYT::NFlow::NCompanion::TJavaCompanionManager",
                resource.getOrThrow("resource_class_name").stringValue());

        YTreeMapNode parameters = companionParameters();
        assertTrue(parameters.getOrThrow("run_process").boolValue());
        assertEquals(
                FlowLauncher.COMPANION_JARS_DIR + File.separator + "*",
                parameters.getOrThrow("classpath").stringValue());
        assertEquals(EXPECTED_JAVA_BIN_PATH, parameters.getOrThrow("jdk_bin_path").stringValue());
        // The hand-written main_class is preserved.
        assertEquals(
                "tech.ytsaurus.flow.tests.NodeCompanionMain",
                parameters.getOrThrow("main_class").stringValue());
    }

    @Test
    void testOverridesPreExistingClasspathAndJdkBinPath() {
        // Pre-populate the hand-written companion parameters with bogus values that the launcher
        // must override; main_class must survive untouched.
        YTreeMapNode parameters = companionParameters();
        parameters.put("classpath", YTree.stringNode("/host/path/that/should/be/overridden/*"));
        parameters.put("jdk_bin_path", YTree.stringNode("/host/path/that/should/be/overridden/java"));

        enrich();

        YTreeMapNode patched = companionParameters();
        assertEquals(
                FlowLauncher.COMPANION_JARS_DIR + File.separator + "*",
                patched.getOrThrow("classpath").stringValue());
        assertEquals(EXPECTED_JAVA_BIN_PATH, patched.getOrThrow("jdk_bin_path").stringValue());
        // The hand-written main_class is preserved.
        assertEquals(
                "tech.ytsaurus.flow.tests.NodeCompanionMain",
                patched.getOrThrow("main_class").stringValue());
    }

    @Test
    void testLayerAndJdkOverridesForHostJdkTest() {
        env.setVar(FlowLauncher.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");
        env.setVar(FlowLauncher.ENV_VAR_JDK_LAYERS, "[]");

        enrich();

        // No layers or system_layer_path on either task for the host-JDK path.
        for (YTreeMapNode task : List.of(controller(), worker())) {
            assertFalse(task.containsKey("layers"));
            assertFalse(task.containsKey("system_layer_path"));
        }
        assertEquals("/usr/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }
}
