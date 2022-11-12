package ru.yandex.yt.canonize_schema;

import java.util.Map;
import java.util.Set;

import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeListNode;
import tech.ytsaurus.ysontree.YTreeMapNode;
import tech.ytsaurus.ysontree.YTreeNode;


public class CanonizeSchema {
    static Set<String> knownAttributes = Set.of(
            "name",
            "type_v3",
            "sort_order",
            "aggregate",
            "expression",
            "lock",
            "group");

    private CanonizeSchema() {
    }

    public static YTreeMapNode canonizeColumnSchema(YTreeMapNode columnSchema) {
        YTreeBuilder result = new YTreeBuilder();

        result.beginMap();
        for (Map.Entry<String, YTreeNode> entry : columnSchema) {
            if (!knownAttributes.contains(entry.getKey())) {
                continue;
            }
            result.key(entry.getKey()).value(entry.getValue());
        }
        return result.buildMap();
    }

    public static YTreeListNode canonizeSchema(YTreeListNode schema) {
        YTreeBuilder result = new YTreeBuilder();
        if (schema.containsAttributes()) {
            result.beginAttributes();
            for (Map.Entry<String, YTreeNode> entry : schema.getAttributes().entrySet()) {
                result.key(entry.getKey()).value(entry.getValue());
            }
            result.endAttributes();
        }

        result.beginList();
        for (YTreeNode node : schema) {
            YTreeMapNode columnSchema = node.mapNode();
            result.value(canonizeColumnSchema(columnSchema));
        }
        return result.buildList();
    }
}
