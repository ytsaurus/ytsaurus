package tech.ytsaurus.client;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YtDecimal;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class YtDecimalTest extends YtClientTestBase {
    @Test
    public void testDecimalWithAnnotation() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        TableSchema schema = new TableSchema.Builder()
                .setUniqueKeys(false)
                .addValue("field", TiType.string())
                .addValue("value", TiType.decimal(7, 3))
                .build();

        yt.createNode(new CreateNode(tablePath.toString(), CypressNodeType.TABLE, Map.of(
                "schema", schema.toYTree()
        ))).get(2, TimeUnit.SECONDS);

        String value = "4571.34";
        TableRow row = new TableRow(value);

        write(yt, tablePath, row);
        List<TableRow> result = read(yt, tablePath);

        assertThat(result.size(), is(1));
        assertThat(result.get(0).value, is(new BigDecimal(value + "0")));
    }

    @Test
    public void testDecimalWithAnnotationNoSchemaInSerializer() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        TableSchema schema = new TableSchema.Builder()
                .setUniqueKeys(false)
                .addValue("field", TiType.string())
                .addValue("value", TiType.decimal(7, 3))
                .build();

        yt.createNode(new CreateNode(tablePath.toString(), CypressNodeType.TABLE, Map.of(
                "schema", schema.toYTree()
        ))).get(2, TimeUnit.SECONDS);

        String value = "4571.34";
        TableRow row = new TableRow(value);

        writeNoSchema(yt, tablePath, row);
        List<TableRow> result = readNoSchema(yt, tablePath);

        assertThat(result.size(), is(1));
        assertThat(result.get(0).value, is(new BigDecimal(value + "0")));
    }

    @Test
    public void testDecimalWithoutAnnotation() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        TableSchema schema = new TableSchema.Builder()
                .setUniqueKeys(false)
                .addValue("field", TiType.string())
                .addValue("value", TiType.decimal(7, 3))
                .build();

        yt.createNode(new CreateNode(tablePath.toString(), CypressNodeType.TABLE, Map.of(
                "schema", schema.toYTree()
        ))).get(2, TimeUnit.SECONDS);

        String value = "4571.34";
        TableRowNoAnnotationParams row = new TableRowNoAnnotationParams(value);

        writeNoAnnotation(yt, tablePath, row);
        List<TableRowNoAnnotationParams> result = readNoAnnotation(yt, tablePath);

        assertThat(result.size(), is(1));
        assertThat(result.get(0).value, is(new BigDecimal(value + "0")));
    }

    @Test
    public void testDecimalWithoutAnnotationNoSchemaInSerializer() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        TableSchema schema = new TableSchema.Builder()
                .setUniqueKeys(false)
                .addValue("field", TiType.string())
                .addValue("value", TiType.decimal(7, 3))
                .build();

        yt.createNode(new CreateNode(tablePath.toString(), CypressNodeType.TABLE, Map.of(
                "schema", schema.toYTree()
        ))).get(2, TimeUnit.SECONDS);

        String value = "4571.34";
        TableRowNoAnnotationParams row = new TableRowNoAnnotationParams(value);

        boolean hasError = false;
        try {
            YTreeObjectSerializerFactory.forClass(TableRowNoAnnotationParams.class);
        } catch (RuntimeException ex) {
            hasError = true;
        }
        Assert.assertTrue(hasError);
    }

    @Test
    public void testWithoutSchemaButWithAnnotation() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        String value = "4571.34";
        TableRow row = new TableRow(value);

        yt.createNode(new CreateNode(tablePath.toString(), CypressNodeType.TABLE)).join();
        write(yt, tablePath, row);
        List<TableRow> result = read(yt, tablePath);

        assertThat(result.size(), is(1));
        assertThat(result.get(0).value, is(new BigDecimal(value + "0")));

        boolean gotError = false;
        try {
            readNoAnnotation(yt, tablePath);
        } catch (Throwable ex) {
            gotError = true;
        }

        assertThat("Error expected", gotError);
    }

    @Test
    public void testWithoutSchemaNoAnnotation() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        String value = "4571.34";
        TableRowNoAnnotationParams row = new TableRowNoAnnotationParams(value);

        boolean gotError = false;
        try {
            writeNoAnnotation(yt, tablePath, row);
        } catch (Throwable ex) {
            gotError = true;
        }

        assertThat("Error expected", gotError);
    }

    private List<TableRowNoAnnotationParams> readNoAnnotation(YtClient yt, YPath tablePath) throws Exception {
        var reader = yt.readTable(
                new ReadTable<>(tablePath, TableRowNoAnnotationParams.class)
        ).join();

        return readImpl(reader);
    }

    private List<TableRow> read(YtClient yt, YPath tablePath) throws Exception {
        var reader = yt.readTable(
                new ReadTable<>(tablePath, TableRow.class)
        ).join();

        return readImpl(reader);
    }

    private List<TableRow> readNoSchema(YtClient yt, YPath tablePath) throws Exception {
        var reader = yt.readTable(
                new ReadTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
        ).join();

        return readImpl(reader);
    }

    private <T> List<T> readImpl(TableReader<T> reader) throws Exception {
        List<T> result = new ArrayList<>();
        try {
            while (reader.canRead()) {
                reader.readyEvent().join();
                List<T> cur;
                while ((cur = reader.read()) != null) {
                    result.addAll(cur);
                }
                reader.readyEvent().join();
            }
        } finally {
            reader.readyEvent().join();
            reader.close().join();
        }

        return result;
    }

    private void write(YtClient yt, YPath tablePath, TableRow row) throws Exception {
        var writer = yt.writeTable(
                new WriteTable<>(tablePath, TableRow.class)
        ).join();

        writeImpl(writer, List.of(row));
    }

    private void writeNoSchema(YtClient yt, YPath tablePath, TableRow row) throws Exception {
        var writer = yt.writeTable(
                new WriteTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
        ).join();

        writeImpl(writer, List.of(row));
    }

    private void writeNoAnnotation(YtClient yt, YPath tablePath, TableRowNoAnnotationParams row)
            throws Exception {
        var writer = yt.writeTable(
                new WriteTable<>(tablePath, TableRowNoAnnotationParams.class)
        ).join();

        writeImpl(writer, List.of(row));
    }

    private <T> void writeImpl(TableWriter<T> writer, List<T> data) throws Exception {
        try {
            writer.readyEvent().join();
            writer.write(data);
        } finally {
            writer.close().join();
        }
    }

    @YTreeObject
    static class TableRow {
        public String field = "123";

        @YtDecimal(precision = 7, scale = 3)
        public BigDecimal value;

        TableRow(String value) {
            this.value = new BigDecimal(value);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TableRow)) {
                return false;
            }
            TableRow other = (TableRow) obj;
            return Objects.equals(field, other.field) && Objects.equals(value, other.value);
        }

        @Override
        public String toString() {
            return String.format("TableRow(\"%s, %s\")", field, value);
        }
    }

    @YTreeObject
    static class TableRowNoAnnotationParams {
        public String field = "123";

        public BigDecimal value;

        TableRowNoAnnotationParams(String value) {
            this.value = new BigDecimal(value);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TableRowNoAnnotationParams)) {
                return false;
            }
            TableRowNoAnnotationParams other = (TableRowNoAnnotationParams) obj;
            return Objects.equals(field, other.field) && Objects.equals(value, other.value);
        }

        @Override
        public String toString() {
            return String.format("TableRowNoAnnotationParams(\"%s, %s\")", field, value);
        }
    }
}
