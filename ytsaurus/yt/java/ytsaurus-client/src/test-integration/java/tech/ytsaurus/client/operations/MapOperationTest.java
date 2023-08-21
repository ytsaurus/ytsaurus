package tech.ytsaurus.client.operations;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.YTsaurusClientTestBase;
import tech.ytsaurus.client.request.MapOperation;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class MapOperationTest extends YTsaurusClientTestBase {
    public static class SimpleMapperYTreeMapNode implements Mapper<YTreeMapNode, YTreeMapNode> {
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

    @Entity
    private static class InputType {
        String name;
        int count;
    }

    @Entity
    private static class OutputType {
        String name;
        @Column(name = "new_count")
        int newCount;
    }

    public static class SimpleMapperEntity implements Mapper<InputType, OutputType> {
        @Override
        public void map(InputType entry, Yield<OutputType> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.name;
            Integer count = entry.count;

            OutputType outputType = new OutputType();
            outputType.name = name;
            outputType.newCount = count * count;

            yield.yield(outputType);
        }
    }

    @Test
    public void testSimpleMapWithYTreeMapNode() {
        testSimple(new SimpleMapperYTreeMapNode());
    }

    @Test
    public void testSimpleMapWithEntity() {
        testSimple(new SimpleMapperEntity());
    }

    private void testSimple(Mapper<?, ?> mapper) {
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
                        .setMapperSpec(new MapperSpec(mapper))
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
