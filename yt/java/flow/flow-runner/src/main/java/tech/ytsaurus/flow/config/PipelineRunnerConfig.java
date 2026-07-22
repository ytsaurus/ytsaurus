package tech.ytsaurus.flow.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;
import tech.ytsaurus.ysontree.YTreeNodeUtils;

import static tech.ytsaurus.flow.config.EnvironmentReader.ENV_VAR_YT_FLOW_CONFIG;
import static tech.ytsaurus.flow.config.EnvironmentReader.ENV_VAR_YT_FLOW_GRACEFUL_UPDATE;
import static tech.ytsaurus.flow.config.EnvironmentReader.ENV_VAR_YT_FLOW_WAIT;
import static tech.ytsaurus.flow.config.EnvironmentReader.ENV_VAR_YT_PROXY_URL_ALIASING_CONFIG;

/**
 * YT Flow pipeline runner configuration.
 * <p>
 * The class extracts configuration from pipeline spec file in yson format and set of environment variables.
 */
public class PipelineRunnerConfig {
    /**
     * Configuration keys.
     */
    // Cluster url of cluster alias.
    public static final String CFG_KEY_CLUSTER_URL = "cluster_url";
    // Path to YT Flow pipeline.
    public static final String CFG_KEY_PATH = "path";
    // Proxy role for YT client.
    public static final String CFG_KEY_PROXY_ROLE = "proxy_role";
    private static final String TRUE = "1";
    private static final Logger log = LoggerFactory.getLogger(PipelineRunnerConfig.class);

    private final EnvironmentReader envReader;
    private final YTreeNode spec;
    private final String clusterAlias;
    private final Optional<String> proxyRole;
    private final String pipelinePath;
    private final String clusterUrl;
    private final boolean gracefulUpdate;
    private final boolean waitForCompletion;
    private @Nullable FlowRunMode runMode;

    /**
     * Construct Config instance.
     *
     * @param configPath A path to pipline spec file in yson format.
     */
    public PipelineRunnerConfig(String configPath) {
        this(configPath, new EnvironmentReader());
    }

    public PipelineRunnerConfig(String configPath, EnvironmentReader envReader) {
        this.envReader = envReader;
        envReader.getVarOptional(EnvironmentReader.ENV_VAR_FLOW_MODE)
                .ifPresent(s -> this.runMode = FlowRunMode.valueOf(s));
        this.spec = loadSpec(configPath);
        Map<String, YTreeNode> config = this.spec.asMap();
        this.clusterAlias = Objects.requireNonNull(config.get(CFG_KEY_CLUSTER_URL)).stringValue();
        this.pipelinePath = Objects.requireNonNull(config.get(CFG_KEY_PATH)).stringValue();
        // proxy_role is optional: a missing key or an explicit entity (#) are both treated as "not set",
        // matching the C++ TSimpleRunnerConfig::ProxyRole (std::optional<std::string>).
        YTreeNode proxyRoleNode = config.get(CFG_KEY_PROXY_ROLE);
        this.proxyRole = (proxyRoleNode == null || proxyRoleNode.isEntityNode())
                ? Optional.empty()
                : Optional.of(proxyRoleNode.stringValue());
        this.clusterUrl = resolveClusterAlias(this.clusterAlias).orElse(this.clusterAlias);
        this.gracefulUpdate = envReader.getVarOptional(ENV_VAR_YT_FLOW_GRACEFUL_UPDATE)
                .map(TRUE::equals)
                .orElse(false);
        this.waitForCompletion = envReader.getVarOptional(ENV_VAR_YT_FLOW_WAIT)
                .map(TRUE::equals)
                .orElse(true);
        validate();
    }

    protected void validate() {
        if (runMode == FlowRunMode.Controller) {
            throw new IllegalArgumentException("Controller mode is not supported yet");
        }
    }

    private YTreeNode loadSpec(String configPath) {
        YsonParser parser;
        try {
            parser = new YsonParser(Files.readAllBytes(Path.of(configPath)));
        } catch (IOException e) {
            log.error("Failed to read config file: ({})", configPath, e);
            throw new UncheckedIOException(e);
        }
        YTreeBuilder builder = YTree.builder();
        parser.parseNode(builder);
        var specBase = builder.build();
        var specPatch = loadConfigPatchFromEnv();
        if (specPatch.isPresent()) {
            log.info("Merging config patch from env");
            var mergedSpecBuilder = YTree.builder();
            YTreeNodeUtils.merge(specBase, specPatch.get(), mergedSpecBuilder, true);
            return mergedSpecBuilder.build();
        }
        return specBase;
    }

    private Optional<YTreeNode> loadConfigPatchFromEnv() {
        return envReader
                .getVarOptional(ENV_VAR_YT_FLOW_CONFIG)
                .map(cfg -> {
                    YsonParser parser = new YsonParser(cfg.getBytes(StandardCharsets.UTF_8));
                    YTreeBuilder builder = YTree.builder();
                    parser.parseNode(builder);
                    return builder.build();
                });
    }

    private Optional<String> resolveClusterAlias(String clusterAlias) {
        return envReader
                .getVarOptional(ENV_VAR_YT_PROXY_URL_ALIASING_CONFIG)
                .map(cfg -> {
                    YsonParser parser = new YsonParser(cfg.getBytes(StandardCharsets.UTF_8));
                    YTreeBuilder builder = YTree.builder();
                    parser.parseNode(builder);
                    YTreeNode node = builder.build();
                    Map<String, YTreeNode> map = node.asMap();
                    return map.get(clusterAlias);
                })
                .map(YTreeNode::stringValue);
    }

    /**
     * A value provided at cluster_url property.
     * It might be a cluster alias or a cluster url.
     *
     * @return Cluster alias.
     */
    public String getClusterAlias() {
        return clusterAlias;
    }

    /**
     * A {@link PipelineRunnerConfig#getClusterAlias()} name resolved by
     * {@link EnvironmentReader#ENV_VAR_YT_PROXY_URL_ALIASING_CONFIG}.
     *
     * @return Resolved cluster url.
     */
    public String getClusterUrl() {
        return clusterUrl;
    }

    public Optional<String> getProxyRole() {
        return proxyRole;
    }

    public String getPipelinePath() {
        return pipelinePath;
    }

    public YTreeNode getFullSpec() {
        return spec;
    }

    /**
     * Controller, Worker or null.
     * If FlowRunMode is null it means that process started in the `runner` mode.
     * <p>
     *
     * @return FlowRunMode.
     */
    public @Nullable FlowRunMode getRunMode() {
        return runMode;
    }

    /**
     * Pipeline static spec located by "spec" key at pipeline spec file.
     *
     * @return Parsed static spec.
     */
    public @Nullable YTreeNode getSpec() {
        return spec.asMap().get("spec");
    }

    /**
     * Pipeline dynamic spec located by "dynamic_spec" key at pipeline spec file.
     *
     * @return Parsed dynamic spec.
     */
    public @Nullable YTreeNode getDynamicSpec() {
        return spec.asMap().get("dynamic_spec");
    }

    /**
     * Flag indicating whether runner must run pipeline in graceful mode.
     * {@code true}: if runner must run pipeline in graceful mode (Drain → Stop → Start).
     * {@code false}: run pipeline in force mode (Pause → Start).
     *
     * @return Graceful update flag.
     */
    public boolean isGracefulUpdate() {
        return gracefulUpdate;
    }

    /**
     * Flag indicating whether runner must wait for pipeline completion.
     *
     * @return Wait for completion flag.
     */
    public boolean isWaitForCompletion() {
        return waitForCompletion;
    }

    @Override
    public String toString() {
        return "Config{" +
                "runMode=" + runMode +
                ", gracefulUpdate=" + gracefulUpdate +
                ", waitForCompletion=" + waitForCompletion +
                ", clusterAlias='" + clusterAlias + '\'' +
                ", proxyRole=" + proxyRole +
                ", pipelinePath='" + pipelinePath + '\'' +
                ", clusterUrl='" + clusterUrl + '\'' +
                ", spec=" + spec +
                '}';
    }
}
