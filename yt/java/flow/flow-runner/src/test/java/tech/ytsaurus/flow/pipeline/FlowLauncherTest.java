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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

    // The java.home of the launcher JVM, the last resort java of a job without a JDK layer.
    private static final String LAUNCHER_JAVA_HOME = "/opt/java/openjdk";

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
                env,
                fakeJars(Path.of("/build/lib/flow-runner.jar"), Path.of("/build/lib/flow-core.jar")),
                LAUNCHER_JAVA_HOME);
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

    /**
     * Writes the in-memory config to a file under the test's temporary directory.
     */
    private Path writeConfig(YTreeNode node) {
        try {
            Path path = tempDir.resolve("pipeline-" + System.nanoTime() + ".yson");
            Files.writeString(path, YTreeTextSerializer.serialize(node));
            return path;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Drives the launcher end-to-end against the parsed test pipeline.
     */
    private void enrich() {
        YTreeMapNode root = config.mapNode();
        launcher.enrichForVanillaLaunch(
                root.getOrThrow("vanilla").mapNode(),
                root.getOrThrow("spec").mapNode());
    }

    /**
     * A jar discovery stubbed to the given jars, without touching the host file system.
     */
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

    /**
     * Declares a minimal controller task in the config, as a hand-written spec would.
     */
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
    void testDeclaredClasspathShipsNoJarsAndSurvives() {
        // A declared classpath names jars the job environment already holds, so nothing is shipped.
        // The java path still follows the job environment: the mounted layer's one wins.
        YTreeMapNode parameters = companionParameters();
        parameters.put("classpath", YTree.stringNode("/app/pipeline/lib/*"));
        parameters.put("jdk_bin_path", YTree.stringNode("/app/pipeline/jdk/bin/java"));

        enrich();

        assertFalse(worker().containsKey("local_files"));
        YTreeMapNode patched = companionParameters();
        assertEquals("/app/pipeline/lib/*", patched.getOrThrow("classpath").stringValue());
        assertEquals(EXPECTED_JAVA_BIN_PATH, patched.getOrThrow("jdk_bin_path").stringValue());
        // The hand-written main_class is preserved.
        assertEquals(
                "tech.ytsaurus.flow.tests.PipelineMain",
                patched.getOrThrow("main_class").stringValue());
    }

    @Test
    void testDeclaredPathsInDockerImageSurviveVerbatim() {
        // The IDP-style image carries both the jars and the JDK at declared paths.
        worker().put("docker_image", YTree.stringNode("registry.example.com/pipeline:1"));
        YTreeMapNode parameters = companionParameters();
        parameters.put("classpath", YTree.stringNode("/app/pipeline/lib/*"));
        parameters.put("jdk_bin_path", YTree.stringNode("/app/pipeline/jdk/bin/java"));

        enrich();

        assertFalse(worker().containsKey("local_files"));
        assertEquals("/app/pipeline/lib/*", companionParameters().getOrThrow("classpath").stringValue());
        assertEquals("/app/pipeline/jdk/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testNoJavaCompanionResourceShipsNoJars() {
        // The jars serve Java companion resources alone.
        companionResource().put(
                "resource_class_name", YTree.stringNode("NYT::NFlow::NCompanion::TCompanionManager"));

        enrich();

        assertFalse(worker().containsKey("local_files"));
    }

    @Test
    void testBlankClasspathIsShipped() {
        companionParameters().put("classpath", YTree.stringNode(" "));

        enrich();

        assertEquals(2, worker().getOrThrow("local_files").asMap().size());
        assertEquals(
                CompanionJars.COMPANION_JARS_DIR + File.separator + "*",
                companionParameters().getOrThrow("classpath").stringValue());
    }

    @Test
    void testMixedResourcesShipJarsAndKeepTheDeclaredClasspath() {
        // One shipment serves the resource without a classpath; the declaring one keeps its own.
        companionParameters().put("classpath", YTree.stringNode("/app/pipeline/lib/*"));
        config.mapNode().getOrThrow("spec").mapNode().getOrThrow("resources").mapNode().put(
                "OtherCompanionManager",
                YTree.mapBuilder()
                        .key("resource_class_name").value(PipelineSpecEnricher.JAVA_COMPANION_MANAGER_CLASS)
                        .key("parameters").beginMap()
                        .key("main_class").value("tech.ytsaurus.flow.tests.OtherMain")
                        .endMap()
                        .buildMap());

        enrich();

        assertEquals(2, worker().getOrThrow("local_files").asMap().size());
        assertEquals("/app/pipeline/lib/*", companionParameters().getOrThrow("classpath").stringValue());
        assertEquals(
                CompanionJars.COMPANION_JARS_DIR + File.separator + "*",
                config.mapNode().getOrThrow("spec").mapNode()
                        .getOrThrow("resources").mapNode()
                        .getOrThrow("OtherCompanionManager").mapNode()
                        .getOrThrow("parameters").mapNode()
                        .getOrThrow("classpath").stringValue());
    }

    @Test
    void testEnvJdkBinPathWinsUnderTheJdkLayer() {
        // The launcher-mounted layer owns the java path: a hand-written one is ignored, the env
        // value replaces the layer's binary.
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
    }

    @Test
    void testDockerImageTakesTheJavaTheLauncherRunsOn() {
        // The launcher runs in the flow-java image of the jobs, so its java.home names the JRE.
        worker().put("docker_image", YTree.stringNode("ghcr.io/ytsaurus/flow-java:0.1.3"));
        launcher = new FlowLauncher(env, fakeJars(Path.of("/build/lib/flow-runner.jar")), "/opt/java/openjdk/");

        enrich();

        assertEquals(
                "/opt/java/openjdk/bin/java",
                companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testHandWrittenJdkBinPathWinsOverLauncherJava() {
        worker().put("docker_image", YTree.stringNode("ghcr.io/ytsaurus/flow-java:0.1.3"));
        companionParameters().put("jdk_bin_path", YTree.stringNode("/opt/custom/jdk/bin/java"));

        enrich();

        assertEquals("/opt/custom/jdk/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testHandWrittenJdkBinPathWinsOverEnvInDockerMode() {
        worker().put("docker_image", YTree.stringNode("ghcr.io/ytsaurus/flow-java:0.1.3"));
        companionParameters().put("jdk_bin_path", YTree.stringNode("/opt/custom/jdk/bin/java"));
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");

        enrich();

        assertEquals("/opt/custom/jdk/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testEnvJdkBinPathWinsOverLauncherJava() {
        worker().put("docker_image", YTree.stringNode("ghcr.io/ytsaurus/flow-java:0.1.3"));
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");

        enrich();

        assertEquals("/usr/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
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
    void testHandWrittenJdkBinPathWinsOverEnvWithExplicitWorkerLayers() {
        worker().put("layers", YTree.listBuilder().value("//porto_layers/custom_jdk.tar.gz").buildList());
        companionParameters().put("jdk_bin_path", YTree.stringNode("/opt/custom/jdk/bin/java"));
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");

        enrich();

        assertEquals("/opt/custom/jdk/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
    }

    @Test
    void testExplicitWorkerLayersTakeEnvJdkBinPath() {
        worker().put("layers", YTree.listBuilder().value("//porto_layers/custom_jdk.tar.gz").buildList());
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "/usr/bin/java");

        enrich();

        assertEquals("/usr/bin/java", companionParameters().getOrThrow("jdk_bin_path").stringValue());
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
    void testBlankHandWrittenJdkBinPathFallsBackToLauncherJava() {
        worker().put("docker_image", YTree.stringNode("ghcr.io/ytsaurus/flow-java:0.1.3"));
        companionParameters().put("jdk_bin_path", YTree.stringNode(" "));

        enrich();

        assertEquals(
                "/opt/java/openjdk/bin/java",
                companionParameters().getOrThrow("jdk_bin_path").stringValue());
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
    void testDisabledJdkLayersTakeTheJavaTheLauncherRunsOn() {
        // Env override: with the layers disabled the job runs on the launcher's host, so the
        // layer's java path gives way to the launcher's own java.
        env.setVar(JobEnvironmentResolver.ENV_VAR_JDK_LAYERS, "[]");
        // A set-but-empty bin path counts as not set.
        env.setVar(JobEnvironment.ENV_VAR_JDK_BIN_PATH, "");

        enrich();

        assertEquals(
                LAUNCHER_JAVA_HOME + "/bin/java",
                companionParameters().getOrThrow("jdk_bin_path").stringValue());
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

        YTreeNode extended = launcher.buildExtendedConfig(pipelinePath, Map.of(words.getStreamId(), words), List.of());

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
                patchedPath.toString(), Map.of(words.getStreamId(), words), List.of());

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
                () -> launcher.buildExtendedConfig(patchedPath.toString(), Map.of(), List.of()));
        assertTrue(error.getMessage().contains("main_class"));
    }

    @Test
    void testDisabledVanillaLeavesTheCompanionResourceUntouched() {
        // A disabled section means the federation is deployed separately, so nothing is patched.
        config.mapNode().getOrThrow("vanilla").mapNode().put("enable", YTree.booleanNode(false));
        Path patchedPath = writeConfig(config);

        YTreeNode extended = launcher.buildExtendedConfig(patchedPath.toString(), Map.of(), List.of());

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

    @Test
    void testExplicitFlowBinWinsOverEnvVar() throws Exception {
        env.setVar(FlowLauncher.ENV_VAR_YT_FLOW_BIN, "/bin/false");

        assertEquals(0, launcher.launch(pipelinePath, "/bin/true", Map.of(), List.of()));
    }

    private Path recordingFlowServer() throws IOException {
        Path script = tempDir.resolve("flow_server");
        Files.writeString(script, """
                #!/bin/sh
                set -eu
                cp "$2" "$0.config"
                printf '%s' "$2" > "$0.config-path"
                """);
        assertTrue(script.toFile().setExecutable(true));
        return script;
    }

    @Test
    void testLaunchUsesTransformedConfigAfterStandardEnrichment() throws Exception {
        Path script = recordingFlowServer();
        byte[] binary = {(byte) 0xff, 0, (byte) 0x80};
        byte[] original = Files.readAllBytes(Path.of(pipelinePath));
        assertEquals(0, launcher.launch(pipelinePath, script.toString(), enriched -> {
            assertTrue(enriched.mapNode().getOrThrow("vanilla").mapNode().getOrThrow("worker")
                    .mapNode().containsKey("local_files"));
            var result = YTree.deepCopy(enriched);
            result.mapNode().put("derived", YTree.bytesNode(binary));
            return result;
        }));
        var submitted = loadConfig(script + ".config");
        assertArrayEquals(binary, submitted.mapNode().getOrThrow("derived").bytesValue());
        assertArrayEquals(original, Files.readAllBytes(Path.of(pipelinePath)));
        Path temporary = Path.of(Files.readString(Path.of(script + ".config-path")));
        assertFalse(Files.exists(temporary.getParent()));
    }

    @Test
    void testRejectedConfigTransformNeverStartsFlowServer() throws Exception {
        Path script = recordingFlowServer();
        assertThrows(IllegalArgumentException.class, () -> launcher.launch(pipelinePath, script.toString(), config -> {
            throw new IllegalArgumentException("Invalid application spec");
        }));
        assertFalse(Files.exists(Path.of(script + ".config")));
    }

    @Test
    void testFinalValidationChecksTheTransformedConfig() throws Exception {
        Path script = recordingFlowServer();
        assertThrows(IllegalStateException.class, () -> launcher.launch(pipelinePath, script.toString(), config -> {
            config.mapNode().getOrThrow("spec").mapNode().getOrThrow("resources").mapNode()
                    .getOrThrow("CompanionManager").mapNode().getOrThrow("parameters").mapNode().remove("main_class");
            return config;
        }));
        assertFalse(Files.exists(Path.of(script + ".config")));
    }

    @Test
    void testNullTransformResultHasContextAndNeverStartsFlowServer() throws Exception {
        Path script = recordingFlowServer();
        var error = assertThrows(
                NullPointerException.class,
                () -> launcher.launch(pipelinePath, script.toString(), config -> null)
        );
        assertTrue(error.getMessage().contains("configTransform must return"), error.getMessage());
        assertFalse(Files.exists(Path.of(script + ".config")));
    }

    @Test
    void testMalformedTransformResultHasContextAndNeverStartsFlowServer() throws Exception {
        Path script = recordingFlowServer();
        for (var result : List.of(
                YTree.node(List.of()),
                YTree.entityNode(),
                YTree.mapBuilder().buildMap(),
                YTree.mapBuilder().key("spec").value(List.of()).buildMap()
        )) {
            var error = assertThrows(
                    IllegalArgumentException.class,
                    () -> launcher.launch(pipelinePath, script.toString(), config -> result)
            );
            assertTrue(error.getMessage().contains("Pipeline config"), error.getMessage());
            assertFalse(Files.exists(Path.of(script + ".config")));
        }
    }

    @Test
    void testTakesFlowBinFromEnvVarWithoutFlag() throws Exception {
        env.setVar(FlowLauncher.ENV_VAR_YT_FLOW_BIN, "/bin/true");

        assertEquals(0, launcher.launch(pipelinePath, null, Map.of(), List.of()));
        assertEquals(0, launcher.launch(pipelinePath, "", Map.of(), List.of()));
        assertEquals(0, launcher.launch(pipelinePath, null, config -> config));
        assertEquals(0, launcher.launch(pipelinePath, "", config -> config));
    }

    @Test
    void testFailsWithoutFlowBinAndEnvVar() {
        var error = assertThrows(
                IllegalArgumentException.class,
                () -> launcher.launch(pipelinePath, null, Map.of(), List.of()));
        assertTrue(error.getMessage().contains("--flow-bin"), error.getMessage());
        assertTrue(error.getMessage().contains(FlowLauncher.ENV_VAR_YT_FLOW_BIN), error.getMessage());
    }

    @Test
    void testEmptyFlowBinEnvVarMeansNotSet() {
        env.setVar(FlowLauncher.ENV_VAR_YT_FLOW_BIN, "");

        assertThrows(
                IllegalArgumentException.class,
                () -> launcher.launch(pipelinePath, null, Map.of(), List.of()));
    }

    @Entity
    @FlowMessage(streamIds = {"words"})
    private static class Word {
        private String word;
    }
}
