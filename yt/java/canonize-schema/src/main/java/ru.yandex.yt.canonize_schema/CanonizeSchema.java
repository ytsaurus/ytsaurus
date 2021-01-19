package ru.yandex.yt.canonize_schema;

import java.util.Map;
import java.util.Set;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeListNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;

public class CanonizeSchema {

    static Set<String> knownAttributes = Set.of(
            "name",
            "type_v3",
            "sort_order",
            "aggregate",
            "expression",
            "lock",
            "group");

    static public YTreeMapNode canonizeColumnSchema(YTreeMapNode columnSchema) {
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

    static public YTreeListNode canonizeSchema(YTreeListNode schema) {
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
