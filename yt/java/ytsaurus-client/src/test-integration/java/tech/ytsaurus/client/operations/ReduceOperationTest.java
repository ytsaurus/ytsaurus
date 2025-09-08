package tech.ytsaurus.client.operations;

import java.util.Iterator;
import java.util.List;

import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.YTsaurusClientTestBase;
import tech.ytsaurus.client.request.ReduceOperation;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.core.operations.OperationContext;
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

    public static class MultiInputIndexReducerYTreeMapNode
            implements ReducerWithKey<YTreeMapNode, YTreeMapNode, Long> {
        @Override
        public Long key(YTreeMapNode entry) {
            return entry.getLong("key");
        }

        @Override
        public void reduce(Long key, Iterator<YTreeMapNode> entries, Yield<YTreeMapNode> yield,
                           Statistics statistics, OperationContext context) {
            while (entries.hasNext()) {
                YTreeMapNode row = entries.next();
                long idx = context.getTableIndex();

                YTreeMapNode outputRow = YTree.builder().beginMap()
                        .key("key").value(key)
                        .key("data").value(row.getString("data"))
                        .key("tableIdx").value(idx)
                        .buildMap();
                yield.yield(outputRow);
            }
        }

        @Override
        public boolean trackIndices() {
            return true;
        }
    }

    private TableSchema createKeyDataSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("key", ColumnValueType.INT64);
        builder.addValue("data", ColumnValueType.STRING);
        return builder.build().toWrite();
    }

    private TableSchema createKeyDataIdxSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("key", ColumnValueType.INT64);
        builder.addValue("data", ColumnValueType.STRING);
        builder.addValue("tableIdx", ColumnValueType.INT64);
        return builder.build().toWrite();
    }

    @Test
    public void testMultiInputTableIndexInContextYTreeMapNode() {
        testMultiInput(new MultiInputIndexReducerYTreeMapNode());
    }

    @Entity
    public static class KeyDataEntityInput {
        public long key;
        public String data;
    }

    @Entity
    public static class TableIndexEntityOutput {
        public long key;
        public String data;
        public long tableIdx;
    }

    public static class MultiInputIndexEntityReducer
            implements ReducerWithKey<KeyDataEntityInput, TableIndexEntityOutput, Long> {
        @Override
        public Long key(KeyDataEntityInput entry) {
            return entry.key;
        }

        @Override
        public void reduce(Long key, Iterator<KeyDataEntityInput> entries, Yield<TableIndexEntityOutput> yield,
                           Statistics statistics, OperationContext context) {
            while (entries.hasNext()) {
                KeyDataEntityInput in = entries.next();
                TableIndexEntityOutput out = new TableIndexEntityOutput();
                out.key = in.key;
                out.data = in.data;
                out.tableIdx = context.getTableIndex();
                yield.yield(out);
            }
        }

        @Override
        public boolean trackIndices() {
            return true;
        }
    }

    @Test
    public void testMultiInputTableIndexInContextEntity() {
        testMultiInput(new MultiInputIndexEntityReducer());
    }

    private void testMultiInput(Reducer<?, ?> reducer) {
        var ytFixture = createYtFixture();
        var yt = ytFixture.getYt();
        var a = ytFixture.getTestDirectory().child("multiTable-a");
        var b = ytFixture.getTestDirectory().child("multiTable-b");
        var out = ytFixture.getTestDirectory().child("multiTable-out");

        var inSchema = createKeyDataSchema();
        var outSchema = createKeyDataIdxSchema();

        writeTable(yt, a, inSchema, List.of(
                YTree.builder().beginMap().key("key").value(1).key("data").value("a").buildMap(),
                YTree.builder().beginMap().key("key").value(2).key("data").value("a").buildMap()
        ));
        writeTable(yt, b, inSchema, List.of(
                YTree.builder().beginMap().key("key").value(1).key("data").value("b").buildMap(),
                YTree.builder().beginMap().key("key").value(2).key("data").value("b").buildMap()
        ));

        Operation sortOp1 = yt.sort(SortOperation.builder()
                .setSpec(SortSpec.builder()
                        .setInputTables(a)
                        .setOutputTable(a)
                        .setSortBy("key")
                        .build())
                .build()).join();
        sortOp1.watchAndThrowIfNotSuccess().join();

        Operation sortOp2 = yt.sort(SortOperation.builder()
                .setSpec(SortSpec.builder()
                        .setInputTables(b)
                        .setOutputTable(b)
                        .setSortBy("key")
                        .build())
                .build()).join();
        sortOp2.watchAndThrowIfNotSuccess().join();

        Operation reduceOp = yt.reduce(ReduceOperation.builder()
                .setSpec(ReduceSpec.builder()
                        .setReducerSpec(new ReducerSpec(reducer))
                        .setInputTables(a, b)
                        .setOutputTables(out.withSchema(outSchema.toYTree()))
                        .setReduceBy("key")
                        .build())
                .build()).join();

        reduceOp.watchAndThrowIfNotSuccess().join();

        List<YTreeMapNode> rows = readTable(yt, out);

        List<YTreeMapNode> expected = List.of(
                YTree.builder().beginMap().key("key").value(1).key("data").value("a").key("tableIdx").value(0)
                        .buildMap(),
                YTree.builder().beginMap().key("key").value(1).key("data").value("b").key("tableIdx").value(1)
                        .buildMap(),
                YTree.builder().beginMap().key("key").value(2).key("data").value("a").key("tableIdx").value(0)
                        .buildMap(),
                YTree.builder().beginMap().key("key").value(2).key("data").value("b").key("tableIdx").value(1)
                        .buildMap()
        );

        Assert.assertEquals(expected, rows);
    }
}
