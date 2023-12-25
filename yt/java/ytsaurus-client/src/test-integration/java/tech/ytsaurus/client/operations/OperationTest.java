package tech.ytsaurus.client.operations;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.YTsaurusClientTestBase;
import tech.ytsaurus.client.request.MapOperation;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;

import static org.apache.logging.log4j.core.util.Throwables.getRootCause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class OperationTest extends YTsaurusClientTestBase {
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

    public static class FreezeMapperEntity implements Mapper<InputType, OutputType> {
        @Override
        public void map(InputType entry, Yield<OutputType> yield, Statistics statistics,
                        OperationContext context) {
            String name = entry.name;
            Integer count = entry.count;

            OutputType outputType = new OutputType();
            outputType.name = name;
            outputType.newCount = 0;
            for (int i = 0; i < count; i++) {
                for (int j = 0; j < count; j++) {
                    outputType.newCount += (i + 1) / count;
                }
            }

            yield.yield(outputType);
        }
    }

    @Test
    public void testWatchAndThrowIfNotSuccess() {
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

        Operation firstOperation = yt.startMap(MapOperation.builder()
                .setSpec(MapSpec.builder()
                        .setMapperSpec(new MapperSpec(new SimpleMapperEntity()))
                        .setInputTables(inputTable)
                        .setOutputTables(outputTable)
                        .build())
                .build()).join();

        firstOperation.watchAndThrowIfNotSuccess().join();

        Assert.assertEquals(OperationStatus.COMPLETED, firstOperation.getStatus().join());

        Operation secondOperation = yt.startMap(MapOperation.builder()
                .setSpec(MapSpec.builder()
                        .setMapperSpec(new MapperSpec(new SimpleMapperEntity()))
                        .setInputTables(YPath.simple("//tmp/this/path/does/not/exist"))
                        .setOutputTables(outputTable)
                        .build())
                .build()).join();

        Exception exception = assertThrows(Exception.class,
                () -> secondOperation.watchAndThrowIfNotSuccess().join()
        );
        assertEquals(
                YTsaurusError.class,
                getRootCause(exception).getClass()
        );
    }

    @Test(timeout = 40000)
    public void testCompleteOperation() throws InterruptedException {
        var ytFixture = createYtFixture();
        var yt = ytFixture.getYt();
        var inputTable = ytFixture.getTestDirectory().child("input-table");
        var outputTable = ytFixture.getTestDirectory().child("output-table");

        var tableSchema = createTableSchema();
        var row = YTree.builder().beginMap()
                .key("name").value("a")
                .key("count").value(Integer.MAX_VALUE)
                .buildMap();
        writeTable(yt, inputTable, tableSchema, List.of(row));

        Operation firstOperation = yt.startMap(MapOperation.builder()
                .setSpec(MapSpec.builder()
                        .setMapperSpec(new MapperSpec(new FreezeMapperEntity()))
                        .setInputTables(inputTable)
                        .setOutputTables(outputTable)
                        .build())
                .build()).join();

        while (firstOperation.getStatus().join() != OperationStatus.RUNNING) {
            Thread.sleep(100);
        }

        firstOperation.complete().join();

        Assert.assertEquals(OperationStatus.COMPLETED, firstOperation.getStatus().join());
    }

    private TableSchema createTableSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("name", ColumnValueType.STRING);
        builder.addValue("count", ColumnValueType.INT64);
        return builder.build().toWrite();
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
}
