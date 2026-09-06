package tech.ytsaurus.flow.pipeline;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import tech.ytsaurus.yson.YsonParser;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * The built-in porto layer set from the {@code yt-porto-layers.yson} classpath resource: the JDK
 * layer matching the runner's Java major version and the base system layer.
 */
final class PortoLayers {
    private static final String RESOURCE = "yt-porto-layers.yson";

    final String jdkLayerPath;
    final String javaBinPath;
    final String systemLayerPath;

    /** Reads the layer config for the running runtime's major version. */
    PortoLayers() {
        YTreeMapNode config;
        try (InputStream stream = PortoLayers.class.getClassLoader().getResourceAsStream(RESOURCE)) {
            if (stream == null) {
                throw new IllegalStateException("Missing classpath resource " + RESOURCE);
            }
            YsonParser parser = new YsonParser(stream.readAllBytes());
            YTreeBuilder builder = YTree.builder();
            parser.parseNode(builder);
            config = builder.build().mapNode();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        int jdkMajorVersion = Runtime.version().feature();
        YTreeNode entry = config.getOrThrow("jdk_layers").mapNode().get(String.valueOf(jdkMajorVersion))
                .orElseThrow(() -> new IllegalStateException(
                        "No JDK layer config for major version " + jdkMajorVersion + " in " + RESOURCE));
        YTreeMapNode entryMap = entry.mapNode();
        this.jdkLayerPath = entryMap.getOrThrow("layer_path").stringValue();
        this.javaBinPath = entryMap.getOrThrow("java_bin_path").stringValue();
        this.systemLayerPath = config.getOrThrow("system_layer_path").stringValue();
    }
}
