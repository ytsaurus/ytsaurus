package tech.ytsaurus.flow.pipeline;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Fills the parts of the static pipeline spec that the SDK can derive from the registered pipeline:
 * the stream schemas. It only ever adds what is missing, so a hand-written spec always wins. The Go
 * counterpart is {@code runner.Enrich}; the C++ one is {@code NYT::NFlow::TSimpleSpecBuilder}.
 *
 * <p>Also validates that every Java companion resource declares a {@code main_class}: the class the
 * worker starts the companion with is set in the pipeline spec, not derived.
 */
public final class PipelineSpecEnricher {

    private static final Logger log = LoggerFactory.getLogger(PipelineSpecEnricher.class);

    static final String JAVA_COMPANION_MANAGER_CLASS = "NYT::NFlow::NCompanion::TJavaCompanionManager";

    private static final String KEY_STREAMS = "streams";
    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_RESOURCES = "resources";
    private static final String KEY_RESOURCE_CLASS_NAME = "resource_class_name";
    private static final String KEY_PARAMETERS = "parameters";
    private static final String KEY_MAIN_CLASS = "main_class";

    private PipelineSpecEnricher() {
    }

    /**
     * Writes the schema of every registered stream into {@code spec.streams}.
     *
     * <p>A stream that the spec does not mention is added; a stream that carries no {@code schema}
     * gets one. A schema already present in the spec always wins — the worker hands the companion
     * the stream specs of the pipeline, so the spec, not the message class, is authoritative at
     * runtime. A difference is only reported: a declared column type may legitimately differ from
     * the derived one (a Java {@code String} field derives {@code utf8}, while specs commonly
     * declare {@code string}).
     *
     * @param spec    the static pipeline spec (the {@code spec} node of the pipeline config).
     * @param streams the streams registered by the pipeline, keyed by stream id.
     */
    static void patchStreamSchemas(YTreeMapNode spec, Map<String, FlowStream<?>> streams) {
        if (streams.isEmpty()) {
            return;
        }

        YTreeMapNode streamSpecs = getOrCreateMap(spec, KEY_STREAMS);
        for (Map.Entry<String, FlowStream<?>> entry : streams.entrySet()) {
            String streamId = entry.getKey();
            TableSchema registered = entry.getValue().getSchema();

            YTreeMapNode streamSpec = getOrCreateMap(streamSpecs, streamId);
            YTreeNode configured = streamSpec.get(KEY_SCHEMA).orElse(null);
            if (configured == null) {
                streamSpec.put(KEY_SCHEMA, registered.toYTree());
                log.info("Inferred schema of stream {} from the registered message type", streamId);
                continue;
            }

            reportSchemaDifference(streamId, configured, registered);
        }
    }

    /**
     * Fails when a companion resource has no class for the worker to start.
     *
     * <p>{@code main_class} defaults to an empty string on the C++ side, so a spec missing it is
     * accepted by {@code flow_server} and only fails once a worker tries to launch the JVM. Catching
     * it here keeps the failure at the launch, where it is actionable.
     *
     * @param spec the static pipeline spec, already enriched.
     * @throws IllegalStateException if a companion resource declares no {@code main_class}.
     */
    static void validateCompanionMainClass(YTreeMapNode spec) {
        for (Map.Entry<String, YTreeMapNode> entry : javaCompanionResources(spec).entrySet()) {
            YTreeNode parameters = entry.getValue().get(KEY_PARAMETERS).orElse(null);
            YTreeMapNode parameterMap = parameters != null && parameters.isMapNode()
                    ? parameters.mapNode()
                    : YTree.mapBuilder().buildMap();
            if (declaredMainClass(parameterMap).isEmpty()) {
                throw new IllegalStateException(
                        ("Companion resource %s declares no main_class;"
                                + " set main_class in the pipeline spec")
                                .formatted(entry.getKey()));
            }
        }
    }

    /** The {@code TJavaCompanionManager} entries of {@code spec.resources}, keyed by resource id. */
    private static Map<String, YTreeMapNode> javaCompanionResources(YTreeMapNode spec) {
        YTreeNode resourcesNode = spec.get(KEY_RESOURCES).orElse(null);
        if (resourcesNode == null || !resourcesNode.isMapNode()) {
            return Map.of();
        }

        var resources = new LinkedHashMap<String, YTreeMapNode>();
        for (Map.Entry<String, YTreeNode> entry : resourcesNode.mapNode().asMap().entrySet()) {
            YTreeNode resource = entry.getValue();
            if (!resource.isMapNode()) {
                continue;
            }
            YTreeMapNode resourceMap = resource.mapNode();
            String className = resourceMap.get(KEY_RESOURCE_CLASS_NAME).map(YTreeNode::stringValue).orElse("");
            if (JAVA_COMPANION_MANAGER_CLASS.equals(className)) {
                resources.put(entry.getKey(), resourceMap);
            }
        }
        return resources;
    }

    /** The declared entry-point class, or an empty string when the spec leaves it out or blank. */
    private static String declaredMainClass(YTreeMapNode parameters) {
        return parameters.get(KEY_MAIN_CLASS)
                .filter(YTreeNode::isStringNode)
                .map(YTreeNode::stringValue)
                .map(String::trim)
                .orElse("");
    }

    /**
     * Logs how a declared stream schema differs from the registered one. Purely diagnostic: a spec
     * may legitimately declare types the client-side schema parser does not model, so a schema that
     * fails to parse is reported and skipped rather than failing the launch.
     */
    private static void reportSchemaDifference(String streamId, YTreeNode configured, TableSchema registered) {
        TableSchema existing;
        try {
            existing = TableSchema.fromYTree(configured);
        } catch (RuntimeException e) {
            log.debug("Stream {}: cannot parse the schema declared in the pipeline config", streamId, e);
            return;
        }
        if (!existing.equals(registered)) {
            log.warn(
                    "Stream {}: the pipeline config declares a schema that differs from the registered "
                            + "message type, keeping the one from the config (config: {}, registered: {})",
                    streamId, existing, registered);
        }
    }

    private static YTreeMapNode getOrCreateMap(YTreeMapNode parent, String key) {
        YTreeNode existing = parent.get(key).orElse(null);
        if (existing != null) {
            if (!existing.isMapNode()) {
                // Repairing a malformed node would submit a different spec than the one written.
                throw new IllegalArgumentException(
                        "The \"%s\" node of the pipeline spec must be a map, got: %s".formatted(key, existing));
            }
            return existing.mapNode();
        }
        YTreeMapNode created = YTree.mapBuilder().buildMap();
        parent.put(key, created);
        return created;
    }
}
