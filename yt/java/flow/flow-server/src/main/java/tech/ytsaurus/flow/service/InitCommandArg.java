package tech.ytsaurus.flow.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.jspecify.annotations.Nullable;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.flow.internal.resource.CompanionResourceInstanceReference;
import tech.ytsaurus.flow.utils.YsonUtils;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Parsed argument of the init command; field names mirror TInitResourceCommandArg.
 */
record InitCommandArg(
        YTreeNode spec,
        YTreeNode dynamicSpec,
        GUID incarnationId,
        long incarnationGeneration,
        long configurationGeneration,
        List<CompanionResourceInstanceReference> dependencies,
        @Nullable YTreeNode resourceRevision
) {

    /**
     * Parses the YSON map argument of an init command.
     *
     * @param argument the command argument, already checked to be a YSON map.
     * @return the parsed argument.
     */
    static InitCommandArg parse(YTreeNode argument) {
        Map<String, YTreeNode> parsed = argument.asMap();
        YTreeNode incarnationIdNode = parsed.get("incarnation_id");
        if (incarnationIdNode == null) {
            throw new IllegalArgumentException("Init argument does not carry an incarnation id");
        }
        List<CompanionResourceInstanceReference> dependencies = new ArrayList<>();
        YTreeNode dependenciesNode = parsed.get("dependencies");
        if (dependenciesNode != null) {
            for (YTreeNode dependencyNode : dependenciesNode.asList()) {
                dependencies.add(parseReference(dependencyNode));
            }
        }
        return new InitCommandArg(
                // Required by TInitResourceCommandArg, which registers neither with a default:
                // silently substituting an empty dynamic spec would load the resource with no
                // dynamic parameters, answer RES_OK, and then reject the corrected payload at the
                // same configuration generation as a conflicting one. An explicitly present empty
                // map stays valid.
                required(parsed, "spec"),
                required(parsed, "dynamic_spec"),
                GUID.valueOf(incarnationIdNode.stringValue()),
                // The generations default to zero rather than being required, matching the wire
                // contract's defaults on the sending side.
                YsonUtils.toLongOrDefault(parsed.get("incarnation_generation"), 0),
                YsonUtils.toLongOrDefault(parsed.get("configuration_generation"), 0),
                List.copyOf(dependencies),
                parsed.get("resource_revision")
        );
    }

    static YTreeNode parseArgument(@Nullable ByteString argument) {
        if (argument == null || argument.isEmpty()) {
            throw new IllegalArgumentException("Resource command argument is required");
        }
        YTreeNode parsed = YsonUtils.yTreeFromProto(argument);
        if (!parsed.isMapNode()) {
            throw new IllegalArgumentException("Resource command argument must be a YSON map");
        }
        return parsed;
    }

    static GUID parseUnload(@Nullable ByteString argument) {
        YTreeNode id = parseArgument(argument).asMap().get("incarnation_id");
        if (id == null) {
            throw new IllegalArgumentException("Unload argument does not carry an incarnation id");
        }
        return GUID.valueOf(id.stringValue());
    }

    private static YTreeNode required(Map<String, YTreeNode> parsed, String key) {
        YTreeNode node = parsed.get(key);
        if (node == null) {
            throw new IllegalArgumentException("Init argument does not carry '%s'".formatted(key));
        }
        return node;
    }

    private static CompanionResourceInstanceReference parseReference(YTreeNode node) {
        Map<String, YTreeNode> map = node.asMap();
        YTreeNode aliasNode = map.get("alias");
        return new CompanionResourceInstanceReference(
                map.get("resource_id").stringValue(),
                GUID.valueOf(map.get("incarnation_id").stringValue()),
                map.get("configuration_generation").longValue(),
                aliasNode != null ? aliasNode.stringValue() : null
        );
    }

    /**
     * The companion-side resource class name from the static spec parameters.
     *
     * @throws IllegalArgumentException if the spec parameters do not name one.
     */
    String companionResourceClass() {
        YTreeNode classNameNode = null;
        if (spec.isMapNode()) {
            YTreeNode parameters = spec.asMap().get("parameters");
            if (parameters != null && parameters.isMapNode()) {
                classNameNode = parameters.asMap().get(ResourceStore.COMPANION_RESOURCE_CLASS_KEY);
            }
        }
        if (classNameNode == null) {
            throw new IllegalArgumentException(
                    "Resource spec parameters do not name a companion resource class under key '%s'".formatted(
                            ResourceStore.COMPANION_RESOURCE_CLASS_KEY)
            );
        }
        return classNameNode.stringValue();
    }

    /**
     * The {@code parameters} map of the static spec; an empty map node when it carries none.
     */
    YTreeNode specParameters() {
        return parameters(spec);
    }

    /**
     * The {@code parameters} map of the dynamic spec; an empty map node when it carries none.
     */
    YTreeNode dynamicSpecParameters() {
        return parameters(dynamicSpec);
    }

    private static YTreeNode parameters(YTreeNode spec) {
        if (spec.isMapNode()) {
            YTreeNode parameters = spec.asMap().get("parameters");
            if (parameters != null && parameters.isMapNode()) {
                return parameters;
            }
        }
        return YTree.builder().beginMap().endMap().build();
    }

    /**
     * The canonical byte form of the carried specs, used for the immutability and conflict checks.
     */
    AppliedSpecs canonicalSpecs() {
        return new AppliedSpecs(
                YsonUtils.serializeYTree(spec),
                YsonUtils.serializeYTree(dynamicSpec),
                resourceRevision != null ? YsonUtils.serializeYTree(resourceRevision) : new byte[0]
        );
    }
}
