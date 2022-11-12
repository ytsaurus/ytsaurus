package ru.yandex.ysonjsonconverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class YsonJsonConverterTest {
    @Test
    public void testYson2Json() {
        YTreeNode source = YTree.builder()
                .beginAttributes()
                .key("attr1").value("value1")
                .key("attr2").value("value2")
                .endAttributes()
                .beginMap()
                .key("key")
                .beginList()
                .beginList()
                .value(1)
                .value(2)
                .value(3)
                .endList()
                .value(148)
                .endList()
                .key("key2").value("value2")
                .endMap()
                .build();

        YTreeBuilder builder = YTree.builder();
        JsonNode node = YsonJsonConverter.yson2json(JsonNodeFactory.instance, source);
        YsonJsonConverter.json2yson(builder, node);
        assertThat(source, equalTo(builder.build()));
    }

}
