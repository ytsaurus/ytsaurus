import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import tech.ytsaurus.client.YtClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YtDecimal;

public class ExampleDecimal {
    private ExampleDecimal() {
    }

    @YTreeObject
    static class TableRow {
        private final String field;
        // The table must have a schema that specifies precision and scale.
        private final BigDecimal value;

        TableRow(String field, BigDecimal value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TableRow(\"%s\", %s)", field, value.toString());
        }
    }

    @YTreeObject
    static class TableRowAnnotated {
        private final String field;

        // Can be used without a schema. Then the precision and scale specified in the annotation
        // will be used for serialization/deserialization.
        @YtDecimal(precision = 7, scale = 3)
        private final BigDecimal value;

        TableRowAnnotated(String field, BigDecimal value) {
            this.field = field;
            this.value = value;
        }

        @Override
        public String toString() {
            return String.format("TableRowAnnotated(\"%s\", %s)", field, value.toString());
        }
    }

    private static <T> void writeRead(YtClient client, YPath path, List<T> data, Class<T> rowClass) throws Exception {
        // Create the writer.
        var writer = client.writeTable(
                WriteTable.<T>builder()
                        .setPath(path)
                        .setNeedRetries(true)
                        .setSerializationContext(
                                new SerializationContext<>(rowClass))
                        .build()).join();

        // Writing data to the table.
        try {
            writer.readyEvent().join();
            writer.write(data);
        } finally {
            writer.close().join();
        }

        // Create the reader.
        var reader = client.readTable(
                ReadTable.<T>builder()
                        .setPath(path)
                        .setSerializationContext(
                                new SerializationContext<>(rowClass))
                        .build()).join();

        // Read the table.
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

        System.out.println("====== READ ROWS ======");
        for (T row : result) {
            System.out.println(row);
        }
        System.out.println("====== END READ ROWS ======");
    }

    public static void main(String[] args) throws Exception {
        TableSchema schema = new TableSchema.Builder()
                .setUniqueKeys(false)
                .addValue("field", TiType.string())
                .addValue("value", TiType.decimal(7, 3))
                .build();

        YtClient client = YtClient.builder()
                .setCluster("hume")
                .build();

        // Columns of type decimal have two parameters - precision and scale.
        // The binary representation of a decimal field depends on these options.
        // There are two options:
        //   - write/read to an existing table with a schema that stores precision and scale
        //   - mark up decimal fields with @YtDecimal, which specifies precision and scale.

        try (client) {
            writeToExistentTable(schema, client);
            writeToNonExistentTable(client);
        }
    }

    private static void writeToExistentTable(TableSchema schema, YtClient client) throws Exception {
        YPath path = YPath.simple("//tmp/" + System.getProperty("user.name") + "-decimal");
        client.createNode(
                CreateNode.builder()
                        .setPath(path)
                        .setType(CypressNodeType.TABLE)
                        .setAttributes(Map.of(
                                "schema", schema.toYTree()
                        ))
                        .setIgnoreExisting(true)
                        .build()).join();

        List<TableRow> data = List.of(
                new TableRow("first", BigDecimal.valueOf(123.45)),
                new TableRow("second", BigDecimal.valueOf(4.123))
        );

        // Writing to the already created table (with the given scheme).
        writeRead(client, path, data, TableRow.class);
    }

    private static void writeToNonExistentTable(YtClient client) throws Exception {
        YPath path = YPath.simple("//tmp/" + System.getProperty("user.name") + "-decimal" + UUID.randomUUID());

        List<TableRowAnnotated> data = List.of(
                new TableRowAnnotated("first", BigDecimal.valueOf(123.45)),
                new TableRowAnnotated("second", BigDecimal.valueOf(4.123))
        );

        // Writing to a non-existent table without a schema,
        // precision and scale are taken from the @YtDecimal annotation.
        writeRead(client, path, data, TableRowAnnotated.class);
    }
}
