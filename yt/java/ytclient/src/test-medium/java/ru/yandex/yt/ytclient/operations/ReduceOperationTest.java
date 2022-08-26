package ru.yandex.yt.ytclient.operations;

import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.proxy.YtClientTestBase;
import ru.yandex.yt.ytclient.proxy.request.ReduceOperation;
import ru.yandex.yt.ytclient.proxy.request.SortOperation;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class ReduceOperationTest extends YtClientTestBase {
    public static class SimpleReducer implements ReducerWithKey<YTreeMapNode, YTreeMapNode, String> {
        @Override
        public String key(YTreeMapNode entry) {
            return entry.getString("name");
        }

        @Override
        public void reduce(String key, Iterator<YTreeMapNode> input, Yield<YTreeMapNode> yield, Statistics statistics) {
            int count = 0;
            while (input.hasNext()) {
                input.next();
                ++count;
            }

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(key)
                    .key("count").value(count)
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
                        .key("field").value("1")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("c")
                        .key("field").value("1")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("a")
                        .key("field").value("2")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("c")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("c")
                        .buildMap()
        ));

        Operation sortOp = yt.sort(SortOperation.builder()
                .setSpec(SortSpec.builder()
                        .setInputTables(inputTable)
                        .setOutputTable(inputTable)
                        .setSortBy("name")
                        .build())
                .build()).join();

        Assert.assertEquals(OperationStatus.COMPLETED, sortOp.getStatus().join());

        Operation op = yt.reduce(ReduceOperation.builder()
                .setSpec(ReduceSpec.builder()
                        .setReducerSpec(new ReducerSpec(new SimpleReducer()))
                        .setInputTables(inputTable)
                        .setOutputTables(outputTable)
                        .setReduceBy("name")
                        .build())
                .build()
        ).join();

        Assert.assertEquals(OperationStatus.COMPLETED, op.getStatus().join());

        List<YTreeMapNode> result = readTable(yt, outputTable);

        List<YTreeMapNode> expected = List.of(
                YTree.builder().beginMap()
                        .key("name").value("a")
                        .key("count").value(2)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .key("count").value(1)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("c")
                        .key("count").value(3)
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
