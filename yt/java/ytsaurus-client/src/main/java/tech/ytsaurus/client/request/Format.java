package tech.ytsaurus.client.request;

import java.util.HashMap;
import java.util.Map;

import tech.ytsaurus.skiff.schema.SkiffSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


public class Format {
    private final String type;
    private final Map<String, YTreeNode> attributes;

    public Format(String type, Map<String, YTreeNode> attributes) {
        this.type = type;
        this.attributes = new HashMap<>(attributes);
    }

    public String getType() {
        return type;
    }

    public Map<String, YTreeNode> getAttributes() {
        return attributes;
    }

    public YTreeNode toTree() {
        YTreeBuilder builder = YTree.builder().beginAttributes();
        for (Map.Entry<String, YTreeNode> attribute : attributes.entrySet()) {
            builder.key(attribute.getKey()).value(attribute.getValue());
        }
        return builder.endAttributes().value(type.toString()).build();
    }

    public static Format ysonBinary() {
        Map<String, YTreeNode> attributes = new HashMap<>();
        attributes.put("format", YTree.stringNode("binary"));
        return new Format("yson", attributes);
    }

    public static Format skiff(SkiffSchema schema) {
        Map<String, YTreeNode> attributes = new HashMap<>();
        String defaultTableName = "table1";
        YTreeNode tableSkiffSchemas = YTree.listBuilder()
                .value("$" + schema.getName().orElse(defaultTableName))
                .endList().build();
        YTreeNode skiffSchemaRegistry = YTree.mapBuilder()
                .key(schema.getName().orElse(defaultTableName)).value(schema.toYTree())
                .endMap().build();
        attributes.put("table_skiff_schemas", tableSkiffSchemas);
        attributes.put("skiff_schema_registry", skiffSchemaRegistry);
        return new Format("skiff", attributes);
    }
}
