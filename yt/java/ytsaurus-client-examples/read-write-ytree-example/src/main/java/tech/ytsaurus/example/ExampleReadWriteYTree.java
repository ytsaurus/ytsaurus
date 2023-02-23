package tech.ytsaurus.example;

import java.util.ArrayList;
import java.util.List;

import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.TableWriter;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeMapNode;

public class ExampleReadWriteYTree {
    private ExampleReadWriteYTree() {
    }

    public static void main(String[] args) {
        YTsaurusClient client = YTsaurusClient.builder()
                .setCluster("freud")
                .build();

        try (client) {
            // The table is located in `//tmp` and contains the name of the current user.
            // The username is necessary in case two people run this example at the same time
            // so that they use different output tables.
            YPath table = YPath.simple("//tmp/" + System.getProperty("user.name") + "-read-write");

            // Write a table.

            // Create the writer.
            TableWriter<YTreeMapNode> writer = client.writeTable(
                    new WriteTable<>(table, YTreeMapNode.class)
            ).join();

            TableSchema tableSchema = TableSchema.builder()
                    .addValue("english", ColumnValueType.STRING)
                    .addValue("russian", ColumnValueType.STRING)
                    .build().toWrite();

            try {
                while (true) {
                    // It is necessary to wait for readyEvent before trying to write.
                    writer.readyEvent().join();

                    // If false is returned, then readyEvent must be waited for before trying again.
                    boolean accepted = writer.write(List.of(
                            YTree.mapBuilder()
                                    .key("english").value("one")
                                    .key("russian").value("один")
                                    .buildMap(),
                            YTree.mapBuilder()
                                    .key("english").value("two")
                                    .key("russian").value("два")
                                    .buildMap()
                    ), tableSchema);

                    if (accepted) {
                        break;
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                // Waiting for completion of writing. An exception might be thrown if something goes wrong.
                writer.close().join();
            }

            // Read a table.

            // Create the reader.
            TableReader<YTreeMapNode> reader = client.readTable(
                    new ReadTable<>(table, YTreeMapNode.class)
            ).join();

            List<YTreeMapNode> rows = new ArrayList<>();

            try {
                // We will read while we can.
                while (reader.canRead()) {
                    // We wait until we can continue reading.
                    reader.readyEvent().join();

                    List<YTreeMapNode> currentRows;
                    while ((currentRows = reader.read()) != null) {
                        rows.addAll(currentRows);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to read");
            } finally {
                reader.close().join();
            }

            for (YTreeMapNode row : rows) {
                System.out.println("russian: " + row.getString("russian") +
                        "; english: " + row.getString("english"));
            }
        }
    }
}
