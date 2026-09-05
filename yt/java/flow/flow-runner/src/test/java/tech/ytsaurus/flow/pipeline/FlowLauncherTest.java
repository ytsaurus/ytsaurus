package tech.ytsaurus.flow.pipeline;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.persistence.Entity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.FlowMessage;
import tech.ytsaurus.flow.stream.FlowStreams;
import tech.ytsaurus.flow.testutils.MockEnvironmentReader;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlowLauncherTest {

    // Mirrors yt-porto-layers.yson. The launcher picks the entry for the JVM it runs on, and that is
    // not always the JDK the module is compiled for: the OpenSource Gradle build has no toolchain and
    // runs the tests on whatever JDK the CI provides.
    private static final Map<Integer, String> EXPECTED_JDK_LAYERS = Map.of(
            17, "//porto_layers/delta/jdk/jdk17/layer_with_jdk17_latest.tar.gz",
            21, "//porto_layers/delta/jdk/jdk21/layer_with_jdk21_latest.tar.gz",
            25, "//porto_layers/delta/jdk/jdk25/layer_with_jdk25_latest.tar.gz");
    private static final Map<Integer, String> EXPECTED_JAVA_BIN_PATHS = Map.of(
            17, "/opt/jdk17/bin/java",
            21, "/opt/jdk21/bin/java",
            25, "/opt/jdk25/bin/java");

    private static final int JDK_MAJOR_VERSION = Runtime.version().feature();
    private static final String EXPECTED_JDK_LAYER = Objects.requireNonNull(
            EXPECTED_JDK_LAYERS.get(JDK_MAJOR_VERSION),
            () -> "No expected JDK layer for major version " + JDK_MAJOR_VERSION);
    private static final String EXPECTED_JAVA_BIN_PATH = Objects.requireNonNull(
            EXPECTED_JAVA_BIN_PATHS.get(JDK_MAJOR_VERSION),
            () -> "No expected java bin path for major version " + JDK_MAJOR_VERSION);
    private static final String EXPECTED_SYSTEM_LAYER =
            "//porto_layers/base/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz";

    @TempDir
    Path tempDir;

    private String pipelinePath;
    private YTreeNode config;
    private MockEnvironmentReader env;
    private FlowLauncher launcher;

    @BeforeEach
    void init() throws URISyntaxException {
        pipelinePath = Path.of(Objects.requireNonNull(
                getClass().getClassLoader().getResource("vanilla_pipeline.yson")).toURI()).toString();
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

    /** Writes the in-memory config to a file under the test's temporary directory. */
    private Path writeConfig(YTreeNode node) {
        try {
            Path path = tempDir.resolve("pipeline-" + System.nanoTime() + ".yson");
            Files.writeString(path, YTreeTextSerializer.serialize(node));
            return path;
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

    /** Declares a minimal controller task in the config, as a hand-written spec would. */
    private void declareController() {
        config.mapNode().getOrThrow("vanilla").mapNode()
                .put("controller", YTree.mapBuilder().key("count").value(1).buildMap());
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
    void testShipsCollidingJarNamesUnderDistinctNames() {
        // Two subprojects may both emit e.g. proto.jar; neither copy may be silently dropped.
        launcher = new FlowLauncher(env) {
            @Override
            protected List<Path> discoverCompanionJars() {
                return List.of(Path.of("/a/proto.jar"), Path.of("/b/proto.jar"));
            }
        };

        enrich();

        Map<String, YTreeNode> localFiles = worker().getOrThrow("local_files").asMap();
        assertEquals(2, localFiles.size());
        assertEquals(
                "/a/proto.jar",
                localFiles.get(FlowLauncher.COMPANION_JARS_DIR + "/proto.jar").stringValue());
        assertEquals(
                "/b/proto.jar",
                localFiles.get(FlowLauncher.COMPANION_JARS_DIR + "/2-proto.jar").stringValue());
    }

    @Test
    void testKeepsUserSuppliedLocalFileOnNameCollision() {
        launcher = new FlowLauncher(env) {
            @Override
            protected List<Path> discoverCompanionJars() {
                return List.of(Path.of("/build/lib/proto.jar"));
            }
        };
        worker().put("local_files", YTree.mapBuilder()
                .key(FlowLauncher.COMPANION_JARS_DIR + "/proto.jar").value("/user/own-proto.jar")
                .buildMap());

        enrich();

        Map<String, YTreeNode> localFiles = worker().getOrThrow("local_files").asMap();
        assertEquals(2, localFiles.size());
        assertEquals(
                "/user/own-proto.jar",
                localFiles.get(FlowLauncher.COMPANION_JARS_DIR + "/proto.jar").stringValue());
        assertEquals(
                "/build/lib/proto.jar",
                localFiles.get(FlowLauncher.COMPANION_JARS_DIR + "/2-proto.jar").stringValue());
    }

    @Test
    void testDiscoversJarsFromLibraryPathDirectories() throws IOException {
        Path libDir = Files.createDirectory(tempDir.resolve("lib"));
        Path libJar = Files.createFile(libDir.resolve("flow-core.jar"));
        Path classpathJar = Files.createFile(tempDir.resolve("app.jar"));

        List<Path> jars = FlowLauncher.discoverCompanionJars(libDir.toString(), classpathJar.toString());

        // The classpath is ignored while the library path holds jars.
        assertEquals(List.of(libJar.toAbsolutePath()), jars);
    }

    @Test
    void testFallsBackToClasspathJarsForPlainLaunch() throws IOException {
        Path appJar = Files.createFile(tempDir.resolve("app.jar"));
        Path depJar = Files.createFile(tempDir.resolve("dep.jar"));
        Path classesDir = Files.createDirectory(tempDir.resolve("classes"));
        String classPath = String.join(
                File.pathSeparator,
                appJar.toString(),
                classesDir.toString(),
                depJar.toString(),
                appJar.toString());

        List<Path> jars = FlowLauncher.discoverCompanionJars("", classPath);

        // Class directories are skipped and repeated entries collapse; the jar order survives.
        assertEquals(List.of(appJar.toAbsolutePath(), depJar.toAbsolutePath()), jars);
    }

    @Test
    void testDiscoveryFailsWithoutAnyJars() throws IOException {
        Path classesDir = Files.createDirectory(tempDir.resolve("classes"));

        var error = assertThrows(
                IllegalStateException.class,
                () -> FlowLauncher.discoverCompanionJars("", classesDir.toString()));
        assertTrue(error.getMessage().contains("java.class.path"));
    }

    @Test
    void testAppliesPortoLayersFromConfigToBothTasks() {
        declareController();

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
        assertEquals(
                FlowLauncher.COMPANION_JARS_DIR + File.separator + "*",
                parameters.getOrThrow("classpath").stringValue());
        assertEquals(EXPECTED_JAVA_BIN_PATH, parameters.getOrThrow("jdk_bin_path").stringValue());
        // The hand-written main_class is preserved.
        assertEquals(
                "tech.ytsaurus.flow.tests.PipelineMain",
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
                "tech.ytsaurus.flow.tests.PipelineMain",
                patched.getOrThrow("main_class").stringValue());
    }

    @Test
    void testBuildExtendedConfigPatchesStreamSchemas() {
        var words = FlowStreams.typed("words", Word.class);

        YTreeNode extended = launcher.buildExtendedConfig(pipelinePath, Map.of(words.getStreamId(), words));

        YTreeMapNode spec = extended.mapNode().getOrThrow("spec").mapNode();
        assertEquals(
                words.getSchema(),
                TableSchema.fromYTree(spec
                        .getOrThrow("streams").mapNode()
                        .getOrThrow("words").mapNode()
                        .getOrThrow("schema")));
        // The hand-written main_class survives every enrichment.
        assertEquals(
                "tech.ytsaurus.flow.tests.PipelineMain",
                spec.getOrThrow("resources").mapNode()
                        .getOrThrow("CompanionManager").mapNode()
                        .getOrThrow("parameters").mapNode()
                        .getOrThrow("main_class").stringValue());
    }

    @Test
    void testBuildExtendedConfigEnrichesSpecWithoutVanilla() {
        config.mapNode().remove("vanilla");
        Path patchedPath = writeConfig(config);
        var words = FlowStreams.typed("words", Word.class);

        YTreeNode extended = launcher.buildExtendedConfig(
                patchedPath.toString(), Map.of(words.getStreamId(), words));

        YTreeMapNode spec = extended.mapNode().getOrThrow("spec").mapNode();
        assertEquals(
                words.getSchema(),
                TableSchema.fromYTree(spec
                        .getOrThrow("streams").mapNode()
                        .getOrThrow("words").mapNode()
                        .getOrThrow("schema")));
        // Without a vanilla block the companion resource is left as written.
        assertFalse(spec.getOrThrow("resources").mapNode()
                .getOrThrow("CompanionManager").mapNode()
                .getOrThrow("parameters").mapNode()
                .containsKey("classpath"));
    }

    @Test
    void testBuildExtendedConfigRejectsSpawnedCompanionWithoutMainClass() {
        // Nothing supplies the entry point: neither the spec nor the caller, so the worker would
        // try to start a JVM with no class to run.
        companionParameters().remove("main_class");
        Path patchedPath = writeConfig(config);

        var error = assertThrows(
                IllegalStateException.class,
                () -> launcher.buildExtendedConfig(patchedPath.toString(), Map.of()));
        assertTrue(error.getMessage().contains("main_class"));
    }

    @Test
    void testDisabledVanillaLeavesTheCompanionResourceUntouched() {
        // A disabled section means the federation is deployed separately, so nothing is patched.
        config.mapNode().getOrThrow("vanilla").mapNode().put("enable", YTree.booleanNode(false));
        Path patchedPath = writeConfig(config);

        YTreeNode extended = launcher.buildExtendedConfig(patchedPath.toString(), Map.of());

        YTreeMapNode parameters = extended.mapNode()
                .getOrThrow("spec").mapNode()
                .getOrThrow("resources").mapNode()
                .getOrThrow("CompanionManager").mapNode()
                .getOrThrow("parameters").mapNode();
        assertFalse(parameters.containsKey("classpath"));
        assertFalse(extended.mapNode().getOrThrow("vanilla").mapNode()
                .getOrThrow("worker").mapNode().containsKey("local_files"));
    }

    @Test
    void testCompanionResourceKeepsUnrelatedKeys() {
        // Completing the parameters must not drop sibling resource keys.
        companionResource().put("dependencies", YTree.mapBuilder().buildMap());

        enrich();

        assertTrue(companionResource().containsKey("dependencies"));
    }

    @Test
    void testLayerAndJdkOverridesForHostJdkTest() {
        declareController();
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

    @Test
    void testAbsentControllerSectionStaysAbsent() {
        enrich();

        assertFalse(config.mapNode().getOrThrow("vanilla").mapNode().containsKey("controller"));
        assertTrue(worker().containsKey("layers"));
    }

    @Test
    void testAbsentControllerSectionStaysAbsentWithHostJdk() {
        env.setVar(FlowLauncher.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");
        env.setVar(FlowLauncher.ENV_VAR_JDK_LAYERS, "[]");

        enrich();

        assertFalse(config.mapNode().getOrThrow("vanilla").mapNode().containsKey("controller"));
        assertFalse(worker().containsKey("layers"));
    }

    @Test
    void testDeleteExtendedConfigRemovesTheTempDir() throws IOException {
        Path extendedConfig = launcher.writeExtendedConfig(config);
        assertTrue(Files.exists(extendedConfig));

        FlowLauncher.deleteExtendedConfig(extendedConfig);

        assertFalse(Files.exists(extendedConfig));
        assertFalse(Files.exists(extendedConfig.getParent()));
        // Deleting an already-removed config is a no-op.
        FlowLauncher.deleteExtendedConfig(extendedConfig);
    }

    @Test
    void testLaunchRemovesTheTempDirAfterFlowServerExits() throws Exception {
        List<Path> written = new ArrayList<>();
        FlowLauncher recording = new FlowLauncher(env) {
            @Override
            protected List<Path> discoverCompanionJars() {
                return List.of(Path.of("/build/lib/flow-runner.jar"));
            }

            @Override
            Path writeExtendedConfig(YTreeNode pipelineConfig) throws IOException {
                Path path = super.writeExtendedConfig(pipelineConfig);
                written.add(path);
                return path;
            }
        };

        assertEquals(0, recording.launch(pipelinePath, "/bin/true", Map.of(), List.of()));

        assertEquals(1, written.size());
        assertFalse(Files.exists(written.get(0).getParent()));
    }

    @Entity
    @FlowMessage(streamIds = {"words"})
    private static class Word {
        private String word;
    }
}
