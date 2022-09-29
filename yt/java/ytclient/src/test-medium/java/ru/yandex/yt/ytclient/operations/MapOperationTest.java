package ru.yandex.yt.ytclient.operations;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.proxy.YtClientTestBase;
import ru.yandex.yt.ytclient.request.MapOperation;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class MapOperationTest extends YtClientTestBase {
    public static class SimpleMapper implements Mapper<YTreeMapNode, YTreeMapNode> {
        @Override
        public void map(YTreeMapNode entry, Yield<YTreeMapNode> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.getString("name");
            Integer count = entry.getInt("count");

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(name)
                    .key("new_count").value(count * count)
                    .buildMap();

            yield.yield(outputRow);
        }
    }

    @Test
    public void testSimple() {
        var ytFixture = createYtFixture();
        var yt = ytFixture.getYt();
        var inputTable = ytFixture.getTestDirectory().child("input-table");
        var outputTable = ytFixture.getTestDirectory().child("output-table");

        var tableSchema = createTableSchema();

        writeTable(yt, inputTable, tableSchema, List.of(
                YTree.builder().beginMap()
                        .key("name").value("a")
                        .key("count").value(1)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .key("count").value(2)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("c")
                        .key("count").value(3)
                        .buildMap()
        ));

        Operation op = yt.map(MapOperation.builder()
                .setSpec(MapSpec.builder()
                        .setMapperSpec(new MapperSpec(new SimpleMapper()))
                        .setInputTables(inputTable)
                        .setOutputTables(outputTable)
                        .build())
                .build()).join();

        Assert.assertEquals(OperationStatus.COMPLETED, op.getStatus().join());

        List<YTreeMapNode> result = readTable(yt, outputTable);

        List<YTreeMapNode> expected = List.of(
                YTree.builder().beginMap()
                        .key("name").value("a")
                        .key("new_count").value(1)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .key("new_count").value(4)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("c")
                        .key("new_count").value(9)
                        .buildMap()
        );

        Assert.assertEquals(expected, result);
    }

    private TableSchema createTableSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("name", ColumnValueType.STRING);
        builder.addValue("count", ColumnValueType.INT64);
        return builder.build().toWrite();
    }
}
