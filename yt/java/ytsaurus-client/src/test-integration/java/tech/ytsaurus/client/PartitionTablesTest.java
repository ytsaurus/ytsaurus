package tech.ytsaurus.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.CreateTablePartitionReader;
import tech.ytsaurus.client.request.MultiTablePartition;
import tech.ytsaurus.client.request.PartitionTables;
import tech.ytsaurus.client.request.PartitionTablesMode;
import tech.ytsaurus.client.request.TablePartitionCookie;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.RangeLimit;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.testlib.LoggingUtils;
import tech.ytsaurus.typeinfo.TiType;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class PartitionTablesTest extends YTsaurusClientTestBase {
    static {
        LoggingUtils.loadJULConfig(
                PartitionTablesTest.class.getResourceAsStream("/logging.properties")
        );
    }

    YTsaurusClient yt;
    YPath tablePath;

    @Before
    public void init() {
        yt = createYtFixture().getYt();
        yt.waitProxies().join();

        tablePath = YPath.simple("//tmp/partition-test-table");

        if (!yt.existsNode(tablePath.justPath().toString()).join()) {
            yt.createNode(tablePath.justPath().toString(), CypressNodeType.TABLE).join();
        }
        var schema = TableSchema.builder().add(new ColumnSchema("value", TiType.string())).build();

        TableWriter<YTreeMapNode> writer = yt.writeTable(new WriteTable<>(tablePath, YTreeMapNode.class)).join();
        try {
            writer.write(List.of(
                    YTree.mapBuilder().key("value").value("value_1").buildMap(),
                    YTree.mapBuilder().key("value").value("value_2").buildMap(),
                    YTree.mapBuilder().key("value").value("value_3").buildMap()
            ), schema);
            writer.close().join();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

        writer = yt.writeTable(new WriteTable<>(tablePath.append(true), YTreeMapNode.class)).join();
        try {
            writer.write(List.of(
                    YTree.mapBuilder().key("value").value("value_4").buildMap(),
                    YTree.mapBuilder().key("value").value("value_5").buildMap(),
                    YTree.mapBuilder().key("value").value("value_6").buildMap()
            ), schema);
            writer.close().join();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void testBasic() {
        // Each row data weight = 7 bytes of data + 1 byte (row constant).
        var result = yt.partitionTables(
                new PartitionTables(List.of(tablePath), PartitionTablesMode.Unordered, DataSize.fromBytes(24))
        ).join();

        var resultPaths = result.stream().map(MultiTablePartition::getTableRanges).collect(Collectors.toList());

        Assert.assertEquals(List.of(
                List.of(tablePath.withRange(RangeLimit.row(0), RangeLimit.row(3))),
                List.of(tablePath.withRange(RangeLimit.row(3), RangeLimit.row(6)))
        ), resultPaths);
    }

    @Test(expected = CompletionException.class)
    public void testMaxPartitionCountExceeded() {
        var request = PartitionTables
                .builder()
                .setPaths(List.of(tablePath))
                .setPartitionMode(PartitionTablesMode.Unordered)
                .setDataWeightPerPartition(DataSize.fromBytes(1))
                .setAdjustDataWeightPerPartition(false)
                .setMaxPartitionCount(1)
                .build();

        var result = yt.partitionTables(request).join();
    }

    @Test
    public void testMaxPartitionCountWithDataWeightAdjustment() {
        var request = PartitionTables
                .builder()
                .setPaths(List.of(tablePath))
                .setPartitionMode(PartitionTablesMode.Unordered)
                .setDataWeightPerPartition(DataSize.fromBytes(1))
                .setMaxPartitionCount(1)
                .build();

        var result = yt.partitionTables(request).join();

        var resultPaths = result.stream().map(MultiTablePartition::getTableRanges).collect(Collectors.toList());

        Assert.assertEquals(List.of(
                List.of(tablePath.withRange(RangeLimit.row(0), RangeLimit.row(6)))
        ), resultPaths);
    }

    @Test
    public void testReadTablePartitionInWireFormat() throws Exception {
        List<MultiTablePartition> partitionsData = yt.partitionTables(
                PartitionTables.builder()
                        .setPaths(List.of(tablePath))
                        .setPartitionMode(PartitionTablesMode.Unordered)
                        .setDataWeightPerPartition(DataSize.fromBytes(24))
                        .setEnableCookies(true)
                        .build()
        ).join();

        List<TablePartitionCookie> cookies = partitionsData.stream().map(MultiTablePartition::getCookie)
                .collect(Collectors.toList());

        var allRows = new ArrayList<UnversionedRow>();
        for (TablePartitionCookie cookie : cookies) {
            CreateTablePartitionReader<UnversionedRow> partitionReaderReq = CreateTablePartitionReader
                    .builder(UnversionedRow.class)
                    .setCookie(cookie)
                    .setUnordered(true)
                    .setOmitInaccessibleColumns(true)
                    .build();
            AsyncReader<UnversionedRow> reader = yt.createTablePartitionReader(partitionReaderReq).join();
            List<UnversionedRow> partitionRows = new ArrayList<>();
            reader.acceptAllAsync(partitionRows::add, Executors.newSingleThreadExecutor()).join();
            reader.close();

            Assert.assertEquals(3, partitionRows.size());
            Assert.assertEquals(3, partitionRows.stream().distinct().count());

            allRows.addAll(partitionRows);
        }

        var expectedData = List.of("value_1", "value_2", "value_3", "value_4", "value_5", "value_6");
        var actualData = allRows.stream()
                .map(row -> row.getValues().get(0).stringValue())
                .sorted()
                .collect(Collectors.toList());

        Assert.assertEquals(expectedData, actualData);
    }

    @Test
    public void testReadTablePartitionWithCustomClass() throws Exception {
        TableSchema schema = TableSchema.builder()
                .addValue("stringValue", TiType.string())
                .addValue("intValue", TiType.int32())
                .build();

        yt.createNode(CreateNode.builder()
                .setPath(tablePath)
                .setType(CypressNodeType.TABLE)
                .setAttributes(Map.of("schema", schema.toYTree()))
                .setIgnoreExisting(true)
                .build()).join();

        List<CustomTableRow> testData = List.of(
                new CustomTableRow("first", 42),
                new CustomTableRow("second", 123),
                new CustomTableRow("third", 789),
                new CustomTableRow("fourth", 1000),
                new CustomTableRow("fifth", 5),
                new CustomTableRow("sixth", 999)
        );

        TableWriter<CustomTableRow> writer = yt.writeTable(
                new WriteTable<>(tablePath, CustomTableRow.class)
        ).join();

        try {
            writer.write(testData);
            writer.close().join();
        } catch (Exception e) {
            writer.close().join();
            throw e;
        }

        List<MultiTablePartition> partitionsData = yt.partitionTables(
                PartitionTables.builder()
                        .setPaths(List.of(tablePath))
                        .setPartitionMode(PartitionTablesMode.Unordered)
                        .setDataWeightPerPartition(DataSize.fromBytes(24))
                        .setEnableCookies(true)
                        .build()
        ).join();

        List<TablePartitionCookie> cookies = partitionsData.stream()
                .map(MultiTablePartition::getCookie)
                .collect(Collectors.toList());

        List<CustomTableRow> allRows = new ArrayList<>();

        for (TablePartitionCookie cookie : cookies) {
            CreateTablePartitionReader<CustomTableRow> partitionReaderReq = CreateTablePartitionReader
                    .builder(CustomTableRow.class)
                    .setCookie(cookie)
                    .setUnordered(true)
                    .setOmitInaccessibleColumns(true)
                    .build();
            AsyncReader<CustomTableRow> partitionReader = yt.createTablePartitionReader(partitionReaderReq).join();
            List<CustomTableRow> partitionRows = new ArrayList<>();
            partitionReader.acceptAllAsync(partitionRows::add, Executors.newSingleThreadExecutor()).join();
            partitionReader.close();

            Assert.assertFalse("Partition should not be empty", partitionRows.isEmpty());

            allRows.addAll(partitionRows);
        }

        Assert.assertEquals("All rows should be read", testData.size(), allRows.size());

        List<CustomTableRow> expectedSorted = new ArrayList<>(testData);
        List<CustomTableRow> actualSorted = new ArrayList<>(allRows);
        expectedSorted.sort(Comparator.comparing(CustomTableRow::getStringValue));
        actualSorted.sort(Comparator.comparing(CustomTableRow::getStringValue));

        Assert.assertEquals("Data should match", expectedSorted, actualSorted);
        Assert.assertEquals("All rows should be distinct",
                allRows.size(), allRows.stream().distinct().count());
    }

    @Entity
    static class CustomTableRow {
        private String stringValue;
        private int intValue;

        CustomTableRow() {
        }

        CustomTableRow(String stringValue, int intValue) {
            this.stringValue = stringValue;
            this.intValue = intValue;
        }

        public String getStringValue() {
            return stringValue;
        }

        public int getIntValue() {
            return intValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CustomTableRow that = (CustomTableRow) o;
            return intValue == that.intValue && Objects.equals(stringValue, that.stringValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(stringValue, intValue);
        }

        @Override
        public String toString() {
            return String.format("CustomTableRow{stringValue='%s', intValue=%d}", stringValue, intValue);
        }
    }
}
