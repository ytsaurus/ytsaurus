package tech.ytsaurus.flow.pipeline;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.config.PipelineRunnerConfig;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeTextSerializer;

/**
 * Java-side runner for the {@code --config --flow-bin} vanilla launch path.
 *
 * <p>Enriches the pipeline spec so the worker ships the Java companion jars and mounts a JDK porto
 * layer, then execs flow_server, which performs the launch and sets the spec.
 */
public class FlowLauncher {
    private static final Logger log = LoggerFactory.getLogger(FlowLauncher.class);

    private static final String JAVA_COMPANION_MANAGER_CLASS = "NYT::NFlow::NCompanion::TJavaCompanionManager";

    private static final String PORTO_LAYERS_RESOURCE = "yt-porto-layers.yson";

    static final String COMPANION_JARS_DIR = "java_companion";

    // Override the JDK binary path (used by the local-YT test).
    static final String ENV_VAR_JDK_BIN_PATH = "YT_FLOW_JDK_BIN_PATH";
    // Override the JDK layers (YSON list); an empty list means no layers and no porto tag.
    static final String ENV_VAR_JDK_LAYERS = "YT_FLOW_JDK_LAYERS";
    // Override the system layer path; empty means do not set it.
    static final String ENV_VAR_SYSTEM_LAYER = "YT_FLOW_SYSTEM_LAYER";

    private final PortoLayersConfig portoLayersConfig;
    private final EnvironmentReader envReader;

    public FlowLauncher() {
        this(new EnvironmentReader());
    }

    FlowLauncher(EnvironmentReader envReader) {
        this.envReader = envReader;
        this.portoLayersConfig = loadPortoLayersConfig();
    }

    /**
     * Enriches the pipeline config, execs flow_server, and returns its exit code.
     */
    public int launch(@Nullable String configPath, @Nullable String flowBin) throws IOException, InterruptedException {
        if (configPath == null || configPath.isEmpty()) {
            throw new IllegalArgumentException("--config <pipeline.yson> is required to launch the pipeline");
        }
        if (flowBin == null || flowBin.isEmpty()) {
            throw new IllegalArgumentException("--flow-bin <path to flow_server> is required to launch the pipeline");
        }

        String flowBinAbs = Paths.get(flowBin).toAbsolutePath().toString();

        PipelineRunnerConfig runnerConfig = new PipelineRunnerConfig(configPath, envReader);
        YTreeNode pipelineConfig = runnerConfig.getFullSpec();
        YTreeMapNode root = pipelineConfig.mapNode();

        YTreeMapNode vanilla = root
                .get("vanilla")
                .filter(YTreeNode::isMapNode)
                .map(YTreeNode::mapNode)
                .orElse(null);

        if (vanilla != null) {
            enrichVanilla(vanilla);
            patchCompanionResources(root.get("spec").get().mapNode());

            configPath = writeExtendedConfig(pipelineConfig).toString();

            log.info("Launching {} with extended config {}", flowBinAbs, configPath);
        }

        return new ProcessBuilder(flowBinAbs, "--config", configPath)
                .inheritIO()
                .start()
                .waitFor();
    }

    /**
     * Enriches the {@code vanilla} section of the pipeline config: ships companion jars into the
     * worker and applies the JDK porto layers to both worker and controller tasks.
     */
    void enrichVanilla(YTreeMapNode vanilla) {
        if (!vanilla.get("enable").map(YTreeNode::boolValue).orElse(false)) {
            return;
        }

        YTreeMapNode worker = getOrCreateMap(vanilla, "worker");
        shipCompanionJars(worker);
        applyLayersAndSystemLayer(worker);

        // Layers are applied to both tasks; jar shipping stays worker-only.
        YTreeMapNode controller = getOrCreateMap(vanilla, "controller");
        applyLayersAndSystemLayer(controller);
    }

    /** Ships every companion jar into {@code worker.local_files}. */
    private void shipCompanionJars(YTreeMapNode worker) {
        YTreeMapNode localFiles = getOrCreateMap(worker, "local_files");
        for (Path jar : discoverCompanionJars()) {
            String inJobName = Path.of(COMPANION_JARS_DIR, jar.getFileName().toString()).toString();
            localFiles.put(inJobName, YTree.stringNode(jar.toAbsolutePath().toString()));
        }
        log.info("Shipping {} companion jars under {}", localFiles.asMap().size(), COMPANION_JARS_DIR);
    }

    /** Enumerates the companion jars from the runner's {@code java.library.path} directories. */
    protected List<Path> discoverCompanionJars() {
        String libPath = System.getProperty("java.library.path", "");
        List<Path> jars = new ArrayList<>();
        for (String entry : libPath.split(File.pathSeparator)) {
            if (entry.isEmpty()) {
                continue;
            }
            Path dir = Paths.get(entry).toAbsolutePath();
            if (!Files.isDirectory(dir)) {
                continue;
            }
            try (Stream<Path> stream = Files.list(dir)) {
                stream.filter(p -> p.getFileName().toString().endsWith(".jar")).forEach(jars::add);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (jars.isEmpty()) {
            throw new IllegalStateException(
                    "No companion jars found under java.library.path=" + libPath
                            + "; cannot ship the Java companion into the vanilla job");
        }
        return jars;
    }

    /** Sets {@code task.layers} and {@code task.system_layer_path}; an empty layer list drops both. */
    private void applyLayersAndSystemLayer(YTreeMapNode task) {
        List<String> jdkLayers = envReader.getVarOptional(ENV_VAR_JDK_LAYERS)
                .map(this::parseJdkLayers)
                .orElseGet(() -> List.of(portoLayersConfig.jdkLayer.layerPath));
        if (jdkLayers.isEmpty()) {
            // No layers: host JDK, no porto tag.
            task.remove("layers");
            task.remove("system_layer_path");
            return;
        }
        YTreeBuilder listBuilder = YTree.listBuilder();
        for (String layer : jdkLayers) {
            listBuilder.value(layer);
        }
        task.put("layers", listBuilder.buildList());

        String systemLayer = envReader.getVarOptional(ENV_VAR_SYSTEM_LAYER).orElse(portoLayersConfig.systemLayerPath);
        if (!systemLayer.isEmpty()) {
            task.put("system_layer_path", YTree.stringNode(systemLayer));
        }
    }

    private List<String> parseJdkLayers(String yson) {
        YTreeNode node = parseYson(yson.getBytes(StandardCharsets.UTF_8));
        List<String> result = new ArrayList<>();
        if (node.isListNode()) {
            for (YTreeNode item : node.asList()) {
                result.add(item.stringValue());
            }
        }
        return result;
    }

    /**
     * Rewrites every {@code TJavaCompanionManager} resource under {@code spec.resources} into a
     * generic {@code TCompanionManager}. Visible for tests.
     */
    void patchCompanionResources(YTreeMapNode spec) {
        YTreeNode resourcesNode = spec.get("resources").orElse(null);
        if (resourcesNode == null || !resourcesNode.isMapNode()) {
            return;
        }
        // Java binary from the layer config, overridable for the local-YT test.
        String jdkBinPath = envReader.getVarOptional(ENV_VAR_JDK_BIN_PATH).orElse(portoLayersConfig.jdkLayer.javaBinPath);
        YTreeMapNode resources = resourcesNode.mapNode();
        for (Map.Entry<String, YTreeNode> entry : resources.asMap().entrySet()) {
            YTreeNode resource = entry.getValue();
            if (!resource.isMapNode()) {
                continue;
            }
            YTreeMapNode resourceMap = resource.mapNode();
            String className = resourceMap.get("resource_class_name").map(YTreeNode::stringValue).orElse("");
            if (!JAVA_COMPANION_MANAGER_CLASS.equals(className)) {
                continue;
            }
            YTreeMapNode generic = buildCompanionManager(resourceMap, jdkBinPath);
            resources.put(entry.getKey(), generic);
            log.info("Rewrote java companion resource {} into a generic TCompanionManager", entry.getKey());
        }
    }

    /** Builds the completed TJavaCompanionManager resource from the hand-written TJavaCompanionManager one. */
    private YTreeMapNode buildCompanionManager(YTreeMapNode javaResource, String jdkBinPath) {
        YTreeMapNode oldParameters = javaResource.get("parameters")
                .filter(YTreeNode::isMapNode)
                .map(YTreeNode::mapNode)
                .orElse(null);

        if (oldParameters == null)
            throw new IllegalArgumentException("Missed parameters in TJavaCompanionManager resource");

        if (!oldParameters.containsKey("main_class"))
            throw new IllegalArgumentException("Missed main_class parameter in TJavaCompanionManager resource");

        YTreeMapNode newParameters = oldParameters.toMapBuilder()
                .key("run_process").value(true)
                .key("classpath").value(COMPANION_JARS_DIR + File.separator + "*")
                .key("jdk_bin_path").value(jdkBinPath)
                .buildMap();

        YTreeBuilder resourceBuilder = YTree.mapBuilder()
                .key("resource_class_name").value(JAVA_COMPANION_MANAGER_CLASS)
                .key("parameters").value(newParameters);

        return resourceBuilder.buildMap();
    }

    private YTreeMapNode getOrCreateMap(YTreeMapNode parent, String key) {
        YTreeNode existing = parent.get(key).orElse(null);
        if (existing != null && existing.isMapNode()) {
            return existing.mapNode();
        }
        YTreeMapNode created = YTree.mapBuilder().buildMap();
        parent.put(key, created);
        return created;
    }

    private YTreeNode parseYson(byte[] bytes) {
        YsonParser parser = new YsonParser(bytes);
        YTreeBuilder builder = YTree.builder();
        parser.parseNode(builder);
        return builder.build();
    }

    /** Reads the YT porto layer config for the running runtime's major version from a classpath resource. */
    private PortoLayersConfig loadPortoLayersConfig() {
        YTreeMapNode config;
        ;
        try (InputStream stream = FlowLauncher.class.getClassLoader().getResourceAsStream(PORTO_LAYERS_RESOURCE)) {
            if (stream == null) {
                throw new IllegalStateException("Missing classpath resource " + PORTO_LAYERS_RESOURCE);
            }
            byte[] bytes = stream.readAllBytes();
            config = parseYson(bytes).mapNode();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String systemLayerPath = config.getOrThrow("system_layer_path").stringValue();
        int jdkMajorVersion = Runtime.version().feature();
        YTreeNode entry = config.getOrThrow("jdk_layers").mapNode().get(String.valueOf(jdkMajorVersion))
                .orElseThrow(() -> new IllegalStateException(
                        "No JDK layer config for major version " + jdkMajorVersion + " in " + PORTO_LAYERS_RESOURCE));
        YTreeMapNode entryMap = entry.mapNode();
        JdkLayer jdkLayer = new JdkLayer(
                entryMap.getOrThrow("layer_path").stringValue(),
                entryMap.getOrThrow("java_bin_path").stringValue());
        return new PortoLayersConfig(jdkLayer, systemLayerPath);
    }

    private Path writeExtendedConfig(YTreeNode pipelineConfig) throws IOException {
        Path dir = Files.createTempDirectory("flow_runner_");
        Path path = dir.resolve("extended-pipeline.yson");
        Files.writeString(path, YTreeTextSerializer.serialize(pipelineConfig), StandardCharsets.UTF_8);
        return path;
    }

    /** Resolved YT porto layer config from {@code yt-porto-layers.yson}. */
    private static final class PortoLayersConfig {
        final JdkLayer jdkLayer;
        final String systemLayerPath;

        PortoLayersConfig(JdkLayer jdkLayer, String systemLayerPath) {
            this.jdkLayer = jdkLayer;
            this.systemLayerPath = systemLayerPath;
        }
    }

    /** Per-JDK-major entry under {@code jdk_layers} in {@code yt-porto-layers.yson}. */
    private static final class JdkLayer {
        final String layerPath;
        final String javaBinPath;

        JdkLayer(String layerPath, String javaBinPath) {
            this.layerPath = layerPath;
            this.javaBinPath = javaBinPath;
        }
    }

}
