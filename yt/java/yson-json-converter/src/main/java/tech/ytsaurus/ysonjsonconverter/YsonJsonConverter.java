package tech.ytsaurus.ysonjsonconverter;

import java.util.Iterator;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * YsonJsonConverter is a utility class for converting yson to json and vice versa.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/storage/formats#json">YTsaurus JSON documentation</a>
 */
public class YsonJsonConverter {
    private YsonJsonConverter() {
    }

    /**
     * Converts a JsonNode to a YTreeNode.
     *
     * @param builder      The YTreeBuilder instance to build the YTreeNode.
     * @param n            The JSON node to be converted.
     * @return             The updated YTreeBuilder instance.
     */
    public static YTreeBuilder json2yson(YTreeBuilder builder, JsonNode n) {
        return json2yson(builder, n, true);
    }

    /**
     * Converts a JsonNode to a YTreeNode with an option to handle special fields.
     *
     * @param builder         The YTreeBuilder instance to build the YTreeNode.
     * @param n               The JsonNode to be converted.
     * @param specialFields   If true, handles special fields like "$attributes" and "$value".
     * @return                The updated YTreeBuilder instance.
     * @throws IllegalArgumentException if the JsonNode type is unexpected.
     */
    public static YTreeBuilder json2yson(YTreeBuilder builder, JsonNode n, boolean specialFields) {
        final JsonNode node;
        if (specialFields && n.isObject() && n.has("$attributes")) {
            JsonNode attrs = n.get("$attributes");

            YTreeBuilder attrsBuilder = json2yson(YTree.builder(), attrs, specialFields);
            YTreeMapNode attrsNode = attrsBuilder.build().mapNode();

            builder = builder.beginAttributes();

            for (Map.Entry<String, YTreeNode> entry : attrsNode.asMap().entrySet()) {
                builder = builder.key(entry.getKey()).value(entry.getValue());
            }

            builder = builder.endAttributes();

            node = n.get("$value");
        } else {
            node = n;
        }

        if (node.isNull()) {
            builder = builder.entity();
        } else if (node.isIntegralNumber()) {
            builder = builder.value(node.longValue());
        } else if (node.isTextual()) {
            builder = builder.value(node.asText());
        } else if (node.isFloatingPointNumber()) {
            builder = builder.value(node.doubleValue());
        } else if (node.isBoolean()) {
            builder = builder.value(node.booleanValue());
        } else if (node.isObject()) {
            builder = builder.beginMap();
            Iterator<String> keys = node.fieldNames();
            while (keys.hasNext()) {
                String key = keys.next();
                builder = builder.key(key).apply(b -> json2yson(b, node.get(key), specialFields));
            }
            builder = builder.endMap();
        } else if (node.isArray()) {
            Iterator<JsonNode> vals = node.elements();
            builder = builder.beginList();
            while (vals.hasNext()) {
                json2yson(builder, vals.next(), specialFields);
            }
            builder = builder.endList();
        } else {
            throw new IllegalArgumentException("Unexpected node type");
        }

        return builder;
    }

    /**
     * Converts a YTreeNode to a corresponding JsonNode.
     *
     * @param factory The JsonNodeFactory instance to create JSON nodes.
     * @param node    The YTreeNode to be converted.
     * @return        The converted JsonNode.
     * @throws IllegalArgumentException if the YTreeNode type is unsupported.
     */
    public static JsonNode yson2json(JsonNodeFactory factory, YTreeNode node) {
        final ObjectNode result = new ObjectNode(factory);
        final Map<String, YTreeNode> attrs = node.getAttributes();
        if (!attrs.isEmpty()) {
            final ObjectNode attrsNode = new ObjectNode(factory);
            for (Map.Entry<String, YTreeNode> entry : attrs.entrySet()) {
                attrsNode.set(entry.getKey(), yson2json(factory, entry.getValue()));
            }
            result.set("$attributes", attrsNode);
        }

        JsonNode valueNode;

        if (node.isEntityNode()) {
            valueNode = NullNode.instance;
        } else if (node.isIntegerNode()) {
            valueNode = new LongNode(node.longValue());
        } else if (node.isStringNode()) {
            valueNode = new TextNode(node.stringValue());
        } else if (node.isDoubleNode()) {
            valueNode = new DoubleNode(node.doubleValue());
        } else if (node.isBooleanNode()) {
            valueNode = node.boolValue()
                    ? BooleanNode.TRUE
                    : BooleanNode.FALSE;
        } else if (node.isMapNode()) {
            final ObjectNode mapNode = new ObjectNode(factory);
            for (Map.Entry<String, YTreeNode> entry : node.asMap().entrySet()) {
                mapNode.set(entry.getKey(), yson2json(factory, entry.getValue()));
            }
            valueNode = mapNode;
        } else if (node.isListNode()) {
            final ArrayNode arrayNode = new ArrayNode(factory);
            for (YTreeNode entry : node.asList()) {
                arrayNode.add(yson2json(factory, entry));
            }
            valueNode = arrayNode;
        } else {
            throw new IllegalArgumentException("");
        }

        if (attrs.isEmpty()) {
            return valueNode;
        } else {
            result.set("$value", valueNode);
            return result;
        }
    }
}
