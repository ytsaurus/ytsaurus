package ru.yandex.yt.ytclient.operations;

import java.util.Iterator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.ytree.YTreeMapNode;
import ru.yandex.yt.ytclient.proxy.YtClientTestBase;
import ru.yandex.yt.ytclient.proxy.request.MapReduceOperation;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class MapReduceOperationTest extends YtClientTestBase {
    public static class SimpleMapper implements Mapper<YTreeMapNode, YTreeMapNode> {
        @Override
        public void map(YTreeMapNode entry, Yield<YTreeMapNode> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.getString("name");

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(name)
                    .key("name_length").value(name.length())
                    .buildMap();

            yield.yield(outputRow);
        }
    }

    public static class SimpleReducer implements ReducerWithKey<YTreeMapNode, YTreeMapNode, String> {
        @Override
        public String key(YTreeMapNode entry) {
            return entry.getString("name");
        }

        @Override
        public void reduce(String key, Iterator<YTreeMapNode> input, Yield<YTreeMapNode> yield, Statistics statistics) {
            int sumNameLength = 0;
            while (input.hasNext()) {
                YTreeMapNode row = input.next();
                sumNameLength += row.getInt("name_length");
            }

            YTreeMapNode outputRow = YTree.builder().beginMap()
                    .key("name").value(key)
                    .key("sum_name_length").value(sumNameLength)
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
                        .key("name").value("aa")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("aa")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .buildMap()
        ));

        Operation op = yt.mapReduce(MapReduceOperation.builder()
                .setSpec(MapReduceSpec.builder()
                        .setReducerSpec(new ReducerSpec(new SimpleReducer()))
                        .setMapperSpec(new MapperSpec(new SimpleMapper()))
                        .setInputTables(inputTable)
                        .setOutputTables(outputTable)
                        .setReduceBy("name")
                        .setSortBy("name")
                        .build())
                .build()
        ).join();

        Assert.assertEquals(OperationStatus.COMPLETED, op.getStatus().join());

        List<YTreeMapNode> result = readTable(yt, outputTable);

        List<YTreeMapNode> expected = List.of(
                YTree.builder().beginMap()
                        .key("name").value("aa")
                        .key("sum_name_length").value(4)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .key("sum_name_length").value(1)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("ccc")
                        .key("sum_name_length").value(9)
                        .buildMap()
        );

        Assert.assertEquals(expected, result);
    }

    private TableSchema createTableSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("name", ColumnValueType.STRING);
        return builder.build().toWrite();
    }
}
