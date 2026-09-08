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
        launcher = new FlowLauncher(
                env, fakeJars(Path.of("/build/lib/flow-runner.jar"), Path.of("/build/lib/flow-core.jar")));
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
        launcher.enrichForVanillaLaunch(
                root.getOrThrow("vanilla").mapNode(),
                root.getOrThrow("spec").mapNode());
    }

    /** A jar discovery stubbed to the given jars, without touching the host file system. */
    private CompanionJars fakeJars(Path... jars) {
        List<Path> list = List.of(jars);
        return new CompanionJars() {
            @Override
            protected List<Path> discover() {
                return list;
            }
        };
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
                localFiles.get(CompanionJars.COMPANION_JARS_DIR + "/flow-runner.jar").stringValue());
        assertEquals(
                "/build/lib/flow-core.jar",
                localFiles.get(CompanionJars.COMPANION_JARS_DIR + "/flow-core.jar").stringValue());
    }

    @Test
    void testShipsCollidingJarNamesUnderDistinctNames() {
        // Two subprojects may both emit e.g. proto.jar; neither copy may be silently dropped.
        launcher = new FlowLauncher(env, fakeJars(Path.of("/a/proto.jar"), Path.of("/b/proto.jar")));

        enrich();

        Map<String, YTreeNode> localFiles = worker().getOrThrow("local_files").asMap();
        assertEquals(2, localFiles.size());
        assertEquals(
                "/a/proto.jar",
                localFiles.get(CompanionJars.COMPANION_JARS_DIR + "/proto.jar").stringValue());
        assertEquals(
                "/b/proto.jar",
                localFiles.get(CompanionJars.COMPANION_JARS_DIR + "/2-proto.jar").stringValue());
    }

    @Test
    void testKeepsUserSuppliedLocalFileOnNameCollision() {
        launcher = new FlowLauncher(env, fakeJars(Path.of("/build/lib/proto.jar")));
        worker().put("local_files", YTree.mapBuilder()
                .key(CompanionJars.COMPANION_JARS_DIR + "/proto.jar").value("/user/own-proto.jar")
                .buildMap());

        enrich();

        Map<String, YTreeNode> localFiles = worker().getOrThrow("local_files").asMap();
        assertEquals(2, localFiles.size());
        assertEquals(
                "/user/own-proto.jar",
                localFiles.get(CompanionJars.COMPANION_JARS_DIR + "/proto.jar").stringValue());
        assertEquals(
                "/build/lib/proto.jar",
                localFiles.get(CompanionJars.COMPANION_JARS_DIR + "/2-proto.jar").stringValue());
    }

    @Test
    void testDiscoversJarsFromLibraryPathDirectories() throws IOException {
        Path libDir = Files.createDirectory(tempDir.resolve("lib"));
        Path libJar = Files.createFile(libDir.resolve("flow-core.jar"));
        Path classpathJar = Files.createFile(tempDir.resolve("app.jar"));

        List<Path> jars = new CompanionJars().discover(libDir.toString(), classpathJar.toString());

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

        List<Path> jars = new CompanionJars().discover("", classPath);

        // Class directories are skipped and repeated entries collapse; the jar order survives.
        assertEquals(List.of(appJar.toAbsolutePath(), depJar.toAbsolutePath()), jars);
    }

    @Test
    void testDiscoveryFailsWithoutAnyJars() throws IOException {
        Path classesDir = Files.createDirectory(tempDir.resolve("classes"));

        var error = assertThrows(
                IllegalStateException.class,
                () -> new CompanionJars().discover("", classesDir.toString()));
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
                CompanionJars.COMPANION_JARS_DIR + File.separator + "*",
                parameters.getOrThrow("classpath").stringValue());
        assertEquals(EXPECTED_JAVA_BIN_PATH, parameters.getOrThrow("jdk_bin_path").stringValue());
        // The hand-written main_class is preserved.
        assertEquals(
                "tech.ytsaurus.flow.tests.PipelineMain",
                parameters.getOrThrow("main_class").stringValue());
    }

    @Test
    void testOverridesPreExistingClasspathAndJdkBinPath() {
        // With the launcher delivering the JDK layer it owns both values: the classpath because it
        // ships the jars, and the java path because only the layer's one exists inside the job;
        // main_class must survive untouched.
        YTreeMapNode parameters = companionParameters();
        parameters.put("classpath", YTree.stringNode("/host/path/that/should/be/overridden/*"));
        parameters.put("jdk_bin_path", YTree.stringNode("/host/path/that/should/be/overridden/java"));

        enrich();

        YTreeMapNode patched = companionParameters();
        assertEquals(
                CompanionJars.COMPANION_JARS_DIR + File.separator + "*",
                patched.getOrThrow("classpath").stringValue());
        assertEquals(EXPECTED_JAVA_BIN_PATH, patched.getOrThrow("jdk_bin_path").stringValue());
        // The hand-written main_class is preserved.
        assertEquals(
                "tech.ytsaurus.flow.tests.PipelineMain",
                patched.getOrThrow("main_class").stringValue());
    }

    @Test
    void testEnvJdkBinPathWinsOverHandWrittenParameters() {
        companionParameters().put("jdk_bin_path", YTree.stringNode("/opt/custom/jdk/bin/java"));
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");

        enrich();

        assertEquals("/usr/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testDockerImageDisablesLayerInjection() {
        declareController();
        // Docker mode is resolved from the vanilla config alone: an image on the worker means the
        // image supplies the JDK, and no task gets porto layers.
        worker().put("docker_image", YTree.stringNode("docker.io/library/eclipse-temurin:17-jre"));
        companionParameters().put("jdk_bin_path", YTree.stringNode("/opt/java/openjdk/bin/java"));

        enrich();

        for (YTreeMapNode task : List.of(controller(), worker())) {
            assertFalse(task.containsKey("layers"));
            assertFalse(task.containsKey("system_layer_path"));
        }
        assertEquals(
                "/opt/java/openjdk/bin/java",
                companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testDockerImageDemandsExplicitJdkBinPath() {
        // The java path inside the image cannot be derived, so the launch must fail up front
        // rather than inside the job.
        worker().put("docker_image", YTree.stringNode("docker.io/library/eclipse-temurin:17-jre"));

        var error = assertThrows(IllegalStateException.class, this::enrich);
        assertTrue(error.getMessage().contains(JobEnvironment.ENV_VAR_JDK_BIN_PATH));
        assertTrue(error.getMessage().contains("jdk_bin_path"));
    }

    @Test
    void testExplicitTaskLayersArePassedThroughVerbatim() {
        declareController();
        // A task with hand-written layers owns its job environment: nothing is injected there,
        // and the layer default java path no longer applies.
        YTreeNode customLayers = YTree.listBuilder().value("//porto_layers/custom_jdk.tar.gz").buildList();
        worker().put("layers", customLayers);
        companionParameters().put("jdk_bin_path", YTree.stringNode("/opt/custom/jdk/bin/java"));

        enrich();

        assertEquals(customLayers, worker().getOrThrow("layers"));
        assertFalse(worker().containsKey("system_layer_path"));
        // The controller has no hand-written layers, so it keeps the default injection.
        assertEquals(
                List.of(EXPECTED_JDK_LAYER),
                controller().getOrThrow("layers").asList().stream().map(YTreeNode::stringValue).toList());
        assertEquals("/opt/custom/jdk/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testExplicitWorkerLayersDemandExplicitJdkBinPath() {
        worker().put("layers", YTree.listBuilder().value("//porto_layers/custom_jdk.tar.gz").buildList());

        var error = assertThrows(IllegalStateException.class, this::enrich);
        assertTrue(error.getMessage().contains(JobEnvironment.ENV_VAR_JDK_BIN_PATH));
    }

    @Test
    void testControllerOnlyDockerImageSwitchesTheWholeLaunch() {
        // Docker mode is global: layers injected into the worker would break the launch on a CRI
        // cluster even when only the controller declares the image.
        config.mapNode().getOrThrow("vanilla").mapNode().put(
                "controller",
                YTree.mapBuilder()
                        .key("docker_image").value("docker.io/library/eclipse-temurin:17-jre")
                        .buildMap());
        companionParameters().put("jdk_bin_path", YTree.stringNode("/opt/java/openjdk/bin/java"));

        enrich();

        for (YTreeMapNode task : List.of(controller(), worker())) {
            assertFalse(task.containsKey("layers"));
            assertFalse(task.containsKey("system_layer_path"));
        }
    }

    @Test
    void testEmptyLayersListMeansNotSet() {
        // As in the C++ config, `layers = []` is the default value, not a hand-written environment.
        worker().put("layers", YTree.listBuilder().buildList());

        enrich();

        assertEquals(
                List.of(EXPECTED_JDK_LAYER),
                worker().getOrThrow("layers").asList().stream().map(YTreeNode::stringValue).toList());
    }

    @Test
    void testBlankHandWrittenJdkBinPathDoesNotSlipPastTheFailFast() {
        worker().put("docker_image", YTree.stringNode("docker.io/library/eclipse-temurin:17-jre"));
        companionParameters().put("jdk_bin_path", YTree.stringNode(" "));

        assertThrows(IllegalStateException.class, this::enrich);
    }

    @Test
    void testHandWrittenSystemLayerPathSurvivesInjection() {
        declareController();
        worker().put("system_layer_path", YTree.stringNode("//porto_layers/custom_base.tar.gz"));

        enrich();

        assertEquals(
                "//porto_layers/custom_base.tar.gz",
                worker().getOrThrow("system_layer_path").stringValue());
        assertEquals(EXPECTED_SYSTEM_LAYER, controller().getOrThrow("system_layer_path").stringValue());
    }

    @Test
    void testEnvJdkLayersOverrideMountsThemOnBothTasks() {
        declareController();
        // The env override wins over the config-driven resolution; the legacy fallback keeps the
        // built-in layer's java path for the companion.
        env.setVar(
                JobEnvironmentResolver.ENV_VAR_JDK_LAYERS,
                "[\"//porto_layers/custom_jdk.tar.gz\"; \"//porto_layers/custom_base.tar.gz\"]");

        enrich();

        for (YTreeMapNode task : List.of(controller(), worker())) {
            assertEquals(
                    List.of("//porto_layers/custom_jdk.tar.gz", "//porto_layers/custom_base.tar.gz"),
                    task.getOrThrow("layers").asList().stream().map(YTreeNode::stringValue).toList());
            assertEquals(EXPECTED_SYSTEM_LAYER, task.getOrThrow("system_layer_path").stringValue());
        }
        assertEquals(EXPECTED_JAVA_BIN_PATH, companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testDisabledJdkLayersDemandExplicitJdkBinPath() {
        // Env override: with the layers disabled the layer's java path points nowhere inside
        // the job, so the launch must fail up front rather than inside the job.
        env.setVar(JobEnvironmentResolver.ENV_VAR_JDK_LAYERS, "[]");

        var error = assertThrows(IllegalStateException.class, this::enrich);
        assertTrue(error.getMessage().contains(JobEnvironment.ENV_VAR_JDK_BIN_PATH));

        // A set-but-empty bin path must not slip past the fail-fast either.
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "");
        assertThrows(IllegalStateException.class, this::enrich);
    }

    @Test
    void testNonListJdkLayersAreRejected() {
        // A non-list value must not silently drop the layers.
        env.setVar(JobEnvironmentResolver.ENV_VAR_JDK_LAYERS, "foo");
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/opt/java/openjdk/bin/java");

        var error = assertThrows(IllegalArgumentException.class, this::enrich);
        assertTrue(error.getMessage().contains(JobEnvironmentResolver.ENV_VAR_JDK_LAYERS));
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
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");
        env.setVar(JobEnvironmentResolver.ENV_VAR_JDK_LAYERS, "[]");

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
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");
        env.setVar(JobEnvironmentResolver.ENV_VAR_JDK_LAYERS, "[]");

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
        FlowLauncher recording = new FlowLauncher(env, fakeJars(Path.of("/build/lib/flow-runner.jar"))) {
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
