package ru.yandex.yt.ytclient.proxy.request;

import java.util.HashMap;
import java.util.Map;

import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
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
        return builder.endAttributes().value(type).build();
    }

    public static Format ysonBinary() {
        Map<String, YTreeNode> attributes = new HashMap<>();
        attributes.put("format", YTree.stringNode("binary"));
        return new Format("yson", attributes);
    }
}
