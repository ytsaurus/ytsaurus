package tech.ytsaurus.client.request;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Message;
import tech.ytsaurus.core.common.YTsaurusProtobufFormat;
import tech.ytsaurus.skiff.SkiffSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


public class Format {
    private static final String DEFAULT_TABLE_NAME = "table";
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
        return builder.endAttributes().value(type).build();
    }

    public static Format ysonBinary() {
        Map<String, YTreeNode> attributes = new HashMap<>();
        attributes.put("format", YTree.stringNode("binary"));
        return new Format("yson", attributes);
    }

    public static Format skiff(SkiffSchema schema, int numberOfTables) {
        Map<String, YTreeNode> attributes = new HashMap<>();
        String tableName = schema.getName().orElse(DEFAULT_TABLE_NAME);

        YTreeBuilder tableSkiffSchemasBuilder = YTree.listBuilder();
        for (int i = 0; i < numberOfTables; i++) {
            tableSkiffSchemasBuilder.value("$" + tableName);
        }

        YTreeNode skiffSchemaRegistry = YTree.mapBuilder()
                .key(tableName).value(schema.toYTree())
                .endMap().build();

        attributes.put("table_skiff_schemas", tableSkiffSchemasBuilder.endList().build());
        attributes.put("skiff_schema_registry", skiffSchemaRegistry);
        return new Format("skiff", attributes);
    }

    public static Format protobuf(Message.Builder messageBuilder) {
        return new Format(
                "protobuf",
                new YTsaurusProtobufFormat(List.of(messageBuilder))
                        .spec().getAttributes()
        );
    }
}
