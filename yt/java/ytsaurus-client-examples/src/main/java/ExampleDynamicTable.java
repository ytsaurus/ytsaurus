import java.util.List;
import java.util.Map;

import tech.ytsaurus.client.ApiServiceTransaction;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.type_info.TiType;
import tech.ytsaurus.ysontree.YTree;


public class ExampleDynamicTable {
    private ExampleDynamicTable() {
    }

    public static void main(String[] args) {
        //
        // The program takes as arguments the name of the cluster and the path to the table it will work with
        // (the table must not exist).
        //
        // By default, users do not have permissions to mount dynamic tables, and you must obtain such permissions
        // (permissions to mount tables) on some YT cluster before starting the program.
        if (args.length != 2) {
            throw new IllegalArgumentException("Incorrect arguments count");
        }
        String cluster = args[0];
        String path = args[1];

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

        YTsaurusClient client = YTsaurusClient.builder()
                .setCluster(cluster)
                .build();

        try (client) {
            // ATTENTION: when creating a dynamic table, we need to
            //  - specify attribute dynamic: true
            //  - specify scheme
            // This must be done in the createNode call.
            // That is, it will not work to create a table first, and then put down these attributes.
            CreateNode createNode = CreateNode.builder()
                    .setPath(YPath.simple(path))
                    .setType(CypressNodeType.TABLE)
                    .setAttributes(Map.of(
                            "dynamic", YTree.booleanNode(true),
                            "schema", schema.toYTree()
                    ))
                    .build();

            client.createNode(createNode).join();

            // To start working with a dynamic table, you need to "mount" it.
            //
            // Creating/mounting/unmounting tables is usually done by dedicated administrative scripts,
            // and the application simply assumes that the tables exist and are already mounted.
            //
            // We will mount the table and unmount it at the end for completeness of the example.
            client.mountTable(path).join();

            // Fill the table.
            try (ApiServiceTransaction transaction =
                         client.startTransaction(new StartTransaction(TransactionType.Tablet)).join()) {
                // Inserting values into a dynamic table can be done using modifyRows.
                transaction.modifyRows(
                        ModifyRowsRequest.builder()
                                .setPath(path)
                                .setSchema(schema)
                                .addInsert(List.of(1, "a"))
                                .addInsert(List.of(2, "b"))
                                .addInsert(List.of(3, "c"))
                                .build()).join();
                transaction.commit().join();
            }

            // Read the table.
            try (ApiServiceTransaction transaction =
                         client.startTransaction(new StartTransaction(TransactionType.Tablet)).join()) {
                // Get values from a dynamic table can be done using lookupRows.
                // It returns UnversionedRows consists of the rows it finds.
                UnversionedRowset rowset = transaction.lookupRows(
                        LookupRowsRequest.builder()
                                .setPath(path)
                                .setSchema(schema.toLookup())
                                .addFilter(1)
                                .build()).join();

                System.out.println("====== LOOKUP RESULT ======");
                for (UnversionedRow row : rowset.getRows()) {
                    System.out.println(row.toYTreeMap(schema, true));
                }
                System.out.println("====== END LOOKUP RESULT ======");

                transaction.commit().join();
            }

            // Unmount the table.
            client.unmountTable(path).join();
        }
    }
}
