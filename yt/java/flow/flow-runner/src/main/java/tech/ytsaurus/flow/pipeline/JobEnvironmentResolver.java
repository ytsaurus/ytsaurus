package tech.ytsaurus.flow.pipeline;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.flow.config.EnvironmentReader;
import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Resolves the {@link JobEnvironment} of a vanilla launch from the hand-written vanilla config and
 * the {@code YT_FLOW_JDK_LAYERS} env override; see the {@link JobEnvironment} javadoc for the
 * rules.
 */
final class JobEnvironmentResolver {
    // Override the resolution (YSON list): a non-empty list is mounted onto the tasks as is, an
    // empty list disables layers entirely — the host-JDK path of the local integration tests.
    static final String ENV_VAR_JDK_LAYERS = "YT_FLOW_JDK_LAYERS";

    private final EnvironmentReader envReader;

    JobEnvironmentResolver(EnvironmentReader envReader) {
        this.envReader = envReader;
    }

    JobEnvironment resolve(YTreeMapNode vanilla) {
        String envLayers = envReader.getVarOptional(ENV_VAR_JDK_LAYERS)
                .filter(value -> !value.isBlank())
                .orElse(null);
        if (envLayers != null) {
            List<String> layers = parseJdkLayers(envLayers);
            if (layers.isEmpty()) {
                // Explicitly disabled: the job environment (docker image or local host) supplies
                // the JDK.
                return new DockerJobEnvironment(envReader);
            }
            PortoLayers porto = new PortoLayers();
            // Legacy contract: custom env layers keep the built-in layer's java path as the
            // default.
            return new PortoJobEnvironment(envReader, layers, porto.systemLayerPath, porto.javaBinPath,
                    /*overrideTaskLayers*/ true);
        }

        if (isDockerMode(vanilla)) {
            return new DockerJobEnvironment(envReader);
        }

        PortoLayers porto = new PortoLayers();
        // A worker that declares its own layers owns its JDK too, so the built-in java path does
        // not apply.
        boolean workerOwnsJdk = vanilla.get("worker")
                .filter(YTreeNode::isMapNode)
                .map(worker -> hasExplicitLayers(worker.mapNode()))
                .orElse(false);
        return new PortoJobEnvironment(
                envReader,
                List.of(porto.jdkLayerPath),
                porto.systemLayerPath,
                /*defaultJdkBinPath*/ workerOwnsJdk ? null : porto.javaBinPath,
                /*overrideTaskLayers*/ false);
    }

    /**
     * True when any vanilla task runs in a docker image. The mode is global: injecting porto
     * layers into the tasks left without an image would break the launch on a CRI cluster.
     */
    private boolean isDockerMode(YTreeMapNode vanilla) {
        for (String taskKey : List.of("worker", "controller")) {
            YTreeNode task = vanilla.get(taskKey).orElse(null);
            if (task == null || !task.isMapNode()) {
                continue;
            }
            YTreeNode image = task.mapNode().get("docker_image").orElse(null);
            if (image != null && image.isStringNode() && !image.stringValue().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    // Mirrors PortoJobEnvironment#hasExplicitLayers; an empty or entity value means "not set",
    // as in C++.
    private boolean hasExplicitLayers(YTreeMapNode task) {
        YTreeNode layers = task.get("layers").orElse(null);
        return layers != null && layers.isListNode() && !layers.asList().isEmpty();
    }

    private List<String> parseJdkLayers(String yson) {
        YsonParser parser = new YsonParser(yson.getBytes(StandardCharsets.UTF_8));
        YTreeBuilder builder = YTree.builder();
        parser.parseNode(builder);
        YTreeNode node = builder.build();
        if (!node.isListNode()) {
            throw new IllegalArgumentException(
                    ENV_VAR_JDK_LAYERS + " must be a YSON list of layer paths, e.g. [] or"
                            + " [\"//porto_layers/jdk\"]; got: " + yson);
        }
        List<String> result = new ArrayList<>();
        for (YTreeNode item : node.asList()) {
            result.add(item.stringValue());
        }
        return result;
    }
}
