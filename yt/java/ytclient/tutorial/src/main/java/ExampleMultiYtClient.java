import tech.ytsaurus.client.MultiYtClient;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

public class ExampleMultiYtClient {
    private ExampleMultiYtClient() {
    }

    public static void main(String[] args) {
        // Schema of the dynamic table to be read
        TableSchema schema = TableSchema.builder()
                .setUniqueKeys(true)
                .add(
                        ColumnSchema.builder("key", TiType.int64())
                                .setSortOrder(ColumnSortOrder.ASCENDING)
                                .build()
                )
                .add(
                        ColumnSchema.builder("value", TiType.string())
                                .build()
                )
                .build();

        // Path where the dynamic table is located. This table is available on two clusters - on hume and freud.
        String path = "//home/dev/tutorial/dynamic-table";

        // We create a client that will first of all make requests to hume,
        // and if there are errors/timeouts, it will make requests to freud.
        MultiYtClient client = MultiYtClient.builder()
                .addCluster("freud")
                .addPreferredCluster("hume")
                .build();

        try (client) {
            // Reading the row with key=2.
            UnversionedRowset rows = client.lookupRows(
                    LookupRowsRequest.builder()
                            .setPath(path)
                            .setSchema(schema.toLookup())
                            .addFilter(2)
                            .build()
            ).join();

            System.out.println("====== LOOKUP RESULT ======");
            for (UnversionedRow row : rows.getRows()) {
                System.out.println(row.toYTreeMap(schema, true));
            }
            System.out.println("====== END LOOKUP RESULT ======");
        }
    }
}
