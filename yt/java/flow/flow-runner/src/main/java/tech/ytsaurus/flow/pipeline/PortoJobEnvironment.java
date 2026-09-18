package tech.ytsaurus.flow.pipeline;

import java.util.List;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Porto job environment: the launcher delivers the JDK by mounting porto layers onto the vanilla
 * tasks, and the companion java binary defaults to the mounted layer's one.
 */
final class PortoJobEnvironment extends AbstractJobEnvironment {
    private static final Logger log = LoggerFactory.getLogger(PortoJobEnvironment.class);

    private final List<String> layers;
    private final String systemLayerPath;
    // Null when the mounted layers carry no known java path (the worker declares its own layers).
    @Nullable
    private final String defaultJdkBinPath;
    // True for the env override, which wins over hand-written task layers.
    private final boolean overrideTaskLayers;

    PortoJobEnvironment(
            EnvironmentReader envReader,
            List<String> layers,
            String systemLayerPath,
            @Nullable String defaultJdkBinPath,
            boolean overrideTaskLayers) {
        super(envReader);
        this.layers = layers;
        this.systemLayerPath = systemLayerPath;
        this.defaultJdkBinPath = defaultJdkBinPath;
        this.overrideTaskLayers = overrideTaskLayers;
    }

    @Override
    protected void patchTaskConfig(YTreeMapNode task) {
        // A task that declares its own layers owns its job environment and survives verbatim.
        if (!overrideTaskLayers && hasExplicitLayers(task)) {
            return;
        }
        YTreeBuilder listBuilder = YTree.listBuilder();
        for (String layer : layers) {
            listBuilder.value(layer);
        }
        task.put("layers", listBuilder.buildList());

        // A hand-written system_layer_path survives.
        if (!task.containsKey("system_layer_path") && !systemLayerPath.isEmpty()) {
            task.put("system_layer_path", YTree.stringNode(systemLayerPath));
        }
    }

    @Override
    protected String doResolveJdkBinPath(@Nullable String handWrittenBinPath) {
        boolean handWritten = handWrittenBinPath != null && !handWrittenBinPath.isBlank();
        if (defaultJdkBinPath != null) {
            // The launcher owns the java path when it delivers the JDK itself; a hand-written path
            // in this mode is a leftover that would point nowhere inside the mounted layer.
            if (handWritten && !handWrittenBinPath.equals(defaultJdkBinPath)) {
                log.warn("Ignoring hand-written jdk_bin_path {}: the launcher mounts a JDK layer with {}",
                        handWrittenBinPath, defaultJdkBinPath);
            }
            return defaultJdkBinPath;
        }
        if (handWritten) {
            return handWrittenBinPath;
        }
        // The worker's hand-written layers carry no known java path, so demand an explicit one up
        // front instead of failing at job runtime.
        throw new IllegalStateException(
                "The worker declares its own layers, so the java binary of the job environment"
                        + " must be set explicitly: put jdk_bin_path into the companion resource"
                        + " parameters or set " + ENV_VAR_JDK_BIN_PATH
                        + ", e.g. /opt/java/openjdk/bin/java");
    }

    // Mirrors JobEnvironmentResolver#hasExplicitLayers; an empty or entity value means "not set",
    // as in C++.
    private boolean hasExplicitLayers(YTreeMapNode task) {
        YTreeNode layers = task.get("layers").orElse(null);
        return layers != null && layers.isListNode() && !layers.asList().isEmpty();
    }
}
