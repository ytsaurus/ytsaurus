package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.yt.ytclient.proxy.request.CreateNode;

@RunWith(value = Parameterized.class)
public class ReadWriteSkiffTypesTest extends ReadWriteTestBase {
    public ReadWriteSkiffTypesTest(YtClient yt) {
        super(yt);
    }

    @Test
    public void testEntityTypesSerialization() {
        YPath table = YPath.simple("//tmp/read-write-skiff-types-test");

        yt.createNode(new CreateNode(table, CypressNodeType.TABLE).setForce(true)).join();

        TableWriter<TableRow> writer = yt.writeTable(
                new WriteTable<>(table, TableRow.class)
        ).join();
        var tableRow = TableRow.newInstance();

        try {
            while (true) {
                writer.readyEvent().join();

                boolean accepted = writer.write(List.of(tableRow));

                if (accepted) {
                    break;
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            writer.close().join();
        }

        TableReader<TableRow> reader = yt.readTable(
                new ReadTable<>(table, TableRow.class)
        ).join();

        List<TableRow> tableRows = new ArrayList<>();

        try {
            while (reader.canRead()) {
                reader.readyEvent().join();

                List<TableRow> currentRows;
                while ((currentRows = reader.read()) != null) {
                    tableRows.addAll(currentRows);
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException("Failed to read");
        } finally {
            reader.close().join();
        }

        Assert.assertEquals(1, tableRows.size());
        Assert.assertEquals(tableRow, tableRows.get(0));
    }

    @Entity
    static class TableRow {
        private byte byteValue;
        private short shortValue;
        private int intValue;
        private long longValue;
        private double doubleValue;
        private boolean booleanValue;
        private String stringValue;
        private YTreeNode ysonValue;
        private NestedEntity nestedEntityValue;
        private @Nullable NestedEntity nullValue;
        private List<List<NestedEntity>> listValue;
        private int[][] arrayValue;

        public static TableRow newInstance() {
            var tableRow = new TableRow();
            tableRow.byteValue = Byte.MAX_VALUE;
            tableRow.shortValue = Short.MAX_VALUE;
            tableRow.intValue = Integer.MAX_VALUE;
            tableRow.longValue = Long.MAX_VALUE;
            tableRow.doubleValue = Double.MAX_VALUE;
            tableRow.booleanValue = true;
            tableRow.stringValue = "any строка 123 \uD83D\uDE00";
            tableRow.ysonValue = YTree.builder()
                    .beginList()
                    .value(new int[]{1, 2, 3})
                    .value(Map.of("one", "один", "two", "два"))
                    .value(YTree.bytesNode(new byte[]{0x00, 0x01}))
                    .endList()
                    .build();
            tableRow.nestedEntityValue = new NestedEntity(Integer.MIN_VALUE);
            tableRow.nullValue = null;
            tableRow.listValue = Stream.of(
                    Stream.of(new NestedEntity(Integer.MIN_VALUE),
                                    null,
                                    new NestedEntity(Integer.MAX_VALUE))
                            .collect(Collectors.toList()),
                    List.of(new NestedEntity(1)),
                    null
            ).collect(Collectors.toList());
            tableRow.arrayValue = new int[][]{
                    new int[]{1, 2, 3},
                    null,
                    new int[]{Integer.MAX_VALUE}
            };
            return tableRow;
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
            return byteValue == tableRow.byteValue &&
                    shortValue == tableRow.shortValue &&
                    intValue == tableRow.intValue &&
                    longValue == tableRow.longValue &&
                    Double.compare(tableRow.doubleValue, doubleValue) == 0 &&
                    booleanValue == tableRow.booleanValue &&
                    stringValue.equals(tableRow.stringValue) &&
                    ysonValue.equals(tableRow.ysonValue) &&
                    nestedEntityValue.equals(tableRow.nestedEntityValue) &&
                    Objects.equals(nullValue, tableRow.nullValue) &&
                    listValue.equals(tableRow.listValue) &&
                    Arrays.deepEquals(arrayValue, tableRow.arrayValue);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(byteValue, shortValue, intValue, longValue, doubleValue, booleanValue,
                    stringValue, ysonValue, nestedEntityValue, nullValue, listValue);
            result = 31 * result + Arrays.deepHashCode(arrayValue);
            return result;
        }
    }

    @Entity
    static class NestedEntity {
        private int intValue;

        NestedEntity() {
        }

        NestedEntity(int intValue) {
            this.intValue = intValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NestedEntity that = (NestedEntity) o;
            return intValue == that.intValue;
        }

        @Override
        public int hashCode() {
            return Objects.hash(intValue);
        }
    }
}
