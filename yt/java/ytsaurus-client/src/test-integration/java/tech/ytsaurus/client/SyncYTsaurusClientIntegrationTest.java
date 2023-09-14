package tech.ytsaurus.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.persistence.Column;
import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.operations.MapSpec;
import tech.ytsaurus.client.operations.Mapper;
import tech.ytsaurus.client.operations.MapperSpec;
import tech.ytsaurus.client.operations.Statistics;
import tech.ytsaurus.client.request.MapOperation;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.sync.SyncYTsaurusClient;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.operations.OperationContext;
import tech.ytsaurus.core.operations.Yield;

import static org.junit.Assert.assertThrows;

public class SyncYTsaurusClientIntegrationTest extends YTsaurusClientTestBase {
    @Test
    public void testReadWrite() {
        var ytFixture = createYtFixture();
        var client = SyncYTsaurusClient.wrap(ytFixture.getYt());

        var table = ytFixture.getTestDirectory().child("sync-yt-read-write");

        List<TableRow> rows = List.of(
                new TableRow("one", "один"),
                new TableRow("two", "два"));
        List<TableRow> receivedRows = new ArrayList<>();

        try (var writer = client.writeTable(new WriteTable<>(table, TableRow.class))) {
            rows.forEach(writer);
        }

        try (var reader = client.readTable(new ReadTable<>(table, TableRow.class))) {
            reader.forEachRemaining(receivedRows::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Assert.assertEquals(rows, receivedRows);
    }

    @Test
    public void testMap() {
        if (false) {
            var ytFixture = createYtFixture();
            var client = SyncYTsaurusClient.wrap(ytFixture.getYt());

            var inputTable = ytFixture.getTestDirectory().child("input-table");
            var outputTable = ytFixture.getTestDirectory().child("output-table");

            var rows = List.of(
                    new InputType("a", 1),
                    new InputType("b", 2)
            );

            try (var writer = client.writeTable(new WriteTable<>(inputTable, InputType.class))) {
                rows.forEach(writer);
            }

            client.map(MapOperation.builder()
                    .setSpec(MapSpec.builder()
                            .setMapperSpec(new MapperSpec(new SimpleMapper()))
                            .setInputTables(inputTable)
                            .setOutputTables(outputTable)
                            .build())
                    .build()
            );

            List<OutputType> receivedRows = new ArrayList<>();
            try (var reader = client.readTable(new ReadTable<>(outputTable, OutputType.class))) {
                reader.forEachRemaining(receivedRows::add);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            var expectedRows = List.of(
                    new OutputType("a", 1),
                    new OutputType("b", 4)
            );

            Assert.assertEquals(expectedRows, receivedRows);

            assertThrows(
                    YTsaurusError.class,
                    () -> client.map(MapOperation.builder()
                            .setSpec(MapSpec.builder()
                                    .setMapperSpec(new MapperSpec(new SimpleMapper()))
                                    .setInputTables(outputTable)
                                    .setOutputTables(inputTable)
                                    .build())
                            .build()
                    )
            );
        }
    }

    @Entity
    static class TableRow {
        private String english;
        private String russian;

        TableRow() {
        }

        TableRow(String english, String russian) {
            this.english = english;
            this.russian = russian;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableRow tableRow = (TableRow) o;
            return Objects.equals(english, tableRow.english) && Objects.equals(russian, tableRow.russian);
        }

        @Override
        public int hashCode() {
            return Objects.hash(english, russian);
        }
    }

    @Entity
    private static class InputType {
        String name;
        int count;

        InputType() {
        }

        InputType(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }

    @Entity
    private static class OutputType {
        String name;
        @Column(name = "new_count")
        int newCount;

        OutputType() {
        }

        OutputType(String name, int newCount) {
            this.name = name;
            this.newCount = newCount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OutputType that = (OutputType) o;
            return newCount == that.newCount && Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, newCount);
        }
    }

    public static class SimpleMapper implements Mapper<InputType, OutputType> {
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
}
