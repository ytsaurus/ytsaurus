package tech.ytsaurus.flow.pipeline;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.flow.config.PipelineRunnerConfig;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.yson.ClosableYsonConsumer;
import tech.ytsaurus.yson.YsonTextWriter;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

/**
 * Java-side runner for the {@code --config --flow-bin} vanilla launch path.
 *
 * <p>Enriches the pipeline spec so the worker ships the Java companion jars and gets a JDK
 * delivered by the job environment resolved from the vanilla config (porto layers or the task's
 * docker image; see the SDK README), then spawns flow_server, which performs the launch and sets
 * the spec, and waits for it.
 */
public class FlowLauncher {
    private static final Logger log = LoggerFactory.getLogger(FlowLauncher.class);

    private final EnvironmentReader envReader;
    private final JobEnvironmentResolver environmentResolver;
    private final CompanionJars companionJars;

    public FlowLauncher() {
        this(new EnvironmentReader());
    }

    FlowLauncher(EnvironmentReader envReader) {
        this(envReader, new CompanionJars());
    }

    FlowLauncher(EnvironmentReader envReader, CompanionJars companionJars) {
        this.envReader = envReader;
        this.environmentResolver = new JobEnvironmentResolver(envReader);
        this.companionJars = companionJars;
    }

    /**
     * Enriches the pipeline config, runs flow_server on it, and returns its exit code.
     */
    public int launch(@Nullable String configPath, @Nullable String flowBin) throws IOException, InterruptedException {
        return launch(configPath, flowBin, Map.of(), List.of());
    }

    /**
     * Enriches the pipeline config with everything the registered pipeline implies, runs
     * flow_server on it, and returns its exit code.
     *
     * @param configPath      path to the pipeline config in YSON format.
     * @param flowBin         path to the {@code flow_server} binary that performs the launch.
     * @param streams         streams registered by the pipeline; their schemas are written into
     *                        {@code spec.streams}.
     * @param flowServerFlags extra flags forwarded to {@code flow_server} verbatim, such as
     *                        {@code --validate-only}.
     */
    public int launch(
            @Nullable String configPath,
            @Nullable String flowBin,
            Map<String, FlowStream<?>> streams,
            List<String> flowServerFlags
    ) throws IOException, InterruptedException {
        if (configPath == null || configPath.isEmpty()) {
            throw new IllegalArgumentException("--config <pipeline.yson> is required to launch the pipeline");
        }
        if (flowBin == null || flowBin.isEmpty()) {
            throw new IllegalArgumentException("--flow-bin <path to flow_server> is required to launch the pipeline");
        }

        String flowBinAbs = Paths.get(flowBin).toAbsolutePath().toString();

        YTreeNode pipelineConfig = buildExtendedConfig(configPath, streams);
        Path extendedConfig = writeExtendedConfig(pipelineConfig);
        try {
            List<String> command = new ArrayList<>(List.of(flowBinAbs, "--config", extendedConfig.toString()));
            command.addAll(flowServerFlags);
            log.info("Launching {}", command);

            Process flowServer = new ProcessBuilder(command)
                    .inheritIO()
                    .start();
            // flow_server mutates the cluster, so it must not outlive the runner watching its exit code.
            // The JVM halts once the hook returns, so the config is deleted here too; flow_server
            // reads it once at startup, so deleting it is safe even after an async destroy.
            Thread terminator = new Thread(() -> {
                flowServer.destroy();
                deleteExtendedConfig(extendedConfig);
            }, "flow-server-terminator");
            Runtime.getRuntime().addShutdownHook(terminator);
            try {
                return flowServer.waitFor();
            } catch (InterruptedException e) {
                flowServer.destroy();
                throw e;
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(terminator);
                } catch (IllegalStateException alreadyShuttingDown) {
                    // Shutdown already started; the hook does the job.
                }
            }
        } finally {
            deleteExtendedConfig(extendedConfig);
        }
    }

    /**
     * Loads the pipeline config and applies every enrichment the launch needs. Visible for tests.
     */
    YTreeNode buildExtendedConfig(String configPath, Map<String, FlowStream<?>> streams) {
        PipelineRunnerConfig runnerConfig = new PipelineRunnerConfig(configPath, envReader);
        YTreeNode pipelineConfig = runnerConfig.getFullSpec();
        YTreeMapNode root = pipelineConfig.mapNode();
        YTreeMapNode spec = root
                .get("spec")
                .filter(YTreeNode::isMapNode)
                .map(YTreeNode::mapNode)
                .orElseThrow(() -> new IllegalArgumentException("Pipeline config has no \"spec\" map"));

        // The registered pipeline is the source of truth for stream schemas, whether or not the
        // runner also submits a vanilla operation.
        PipelineSpecEnricher.patchStreamSchemas(spec, streams);

        YTreeMapNode vanilla = root
                .get("vanilla")
                .filter(YTreeNode::isMapNode)
                .map(YTreeNode::mapNode)
                .orElse(null);

        if (vanilla != null && vanilla.get("enable").map(YTreeNode::boolValue).orElse(false)) {
            enrichForVanillaLaunch(vanilla, spec);
        }

        // Last: the final gate validates the spec exactly as it is submitted.
        PipelineSpecEnricher.validateCompanionMainClass(spec);

        return pipelineConfig;
    }

    /**
     * Applies every vanilla-launch enrichment: ships the companion jars, applies the resolved
     * job environment to the tasks, and completes the companion resources. Visible for tests.
     */
    void enrichForVanillaLaunch(YTreeMapNode vanilla, YTreeMapNode spec) {
        JobEnvironment environment = environmentResolver.resolve(vanilla);

        YTreeMapNode worker = getOrCreateMap(vanilla, "worker");
        companionJars.ship(worker);

        environment.patchVanillaConfig(vanilla);
        PipelineSpecEnricher.patchCompanionResources(spec, environment);
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

    /** Writes the enriched config into a fresh temp dir. Visible for tests. */
    Path writeExtendedConfig(YTreeNode pipelineConfig) throws IOException {
        Path dir = Files.createTempDirectory("flow_runner_");
        Path path = dir.resolve("extended-pipeline.yson");
        // YTreeTextSerializer decodes every string node through a Java String,
        // which corrupts non-UTF-8 payloads (e.g. a serialized proto descriptor
        // set in the spec); walking with stringAsBytes escapes the raw bytes
        // instead, producing pure-ASCII text.
        StringBuilder sb = new StringBuilder();
        try (ClosableYsonConsumer consumer = new YsonTextWriter(sb)) {
            YTreeNodeUtils.walk(pipelineConfig, consumer, /*stringAsBytes*/ true);
        }
        Files.writeString(path, sb.toString(), StandardCharsets.UTF_8);
        return path;
    }

    /** Removes the config written by {@link #writeExtendedConfig} together with its temp dir. Visible for tests. */
    static void deleteExtendedConfig(Path extendedConfig) {
        try {
            Files.deleteIfExists(extendedConfig);
            Files.deleteIfExists(extendedConfig.getParent());
        } catch (IOException e) {
            // Best effort: a leftover temp dir is litter, not a launch failure.
            log.warn("Failed to delete temp config dir {}", extendedConfig.getParent(), e);
        }
    }
}
