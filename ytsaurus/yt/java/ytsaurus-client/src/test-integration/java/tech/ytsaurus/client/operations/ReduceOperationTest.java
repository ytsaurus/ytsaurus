package tech.ytsaurus.client.operations;

import java.util.Iterator;
import java.util.List;

import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.YTsaurusClientTestBase;
import tech.ytsaurus.client.request.ReduceOperation;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;


public class ReduceOperationTest extends YTsaurusClientTestBase {
    public static class SimpleReducerYTreeMapNode implements ReducerWithKey<YTreeMapNode, YTreeMapNode, String> {
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

    @Entity
    private static class InputType {
        String name;
        String field;
    }

    @Entity
    private static class OutputType {
        String name;
        int count;
    }

    public static class SimpleReducerEntity implements ReducerWithKey<InputType, OutputType, String> {
        @Override
        public String key(InputType entry) {
            return entry.name;
        }

        @Override
        public void reduce(String key, Iterator<InputType> input, Yield<OutputType> yield, Statistics statistics) {
            int count = 0;
            while (input.hasNext()) {
                input.next();
                ++count;
            }

            OutputType outputType = new OutputType();
            outputType.name = key;
            outputType.count = count;

            yield.yield(outputType);
        }
    }

    @Test
    public void testSimpleReduceWithYTreeMapNode() {
        testSimple(new SimpleReducerEntity());
    }

    @Test
    public void testSimpleReduceWithEntity() {
        testSimple(new SimpleReducerYTreeMapNode());
    }

    private void testSimple(Reducer<?, ?> reducer) {
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
                        .setReducerSpec(new ReducerSpec(reducer))
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
