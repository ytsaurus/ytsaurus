package tech.ytsaurus.example;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Entity;

import tech.ytsaurus.client.TableReader;
import tech.ytsaurus.client.TableWriter;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.YPath;

public class ExampleReadWriteEntity {
    private ExampleReadWriteEntity() {
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
        public String toString() {
            return String.format("TableRow(\"%s, %s\")", english, russian);
        }
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
            TableWriter<TableRow> writer = client.writeTable(
                    WriteTable.<TableRow>builder()
                            .setPath(table)
                            .setSerializationContext(new SerializationContext<>(TableRow.class))
                            .setNeedRetries(true)
                            .build()).join();

            try {
                while (true) {
                    // It is necessary to wait for readyEvent before trying to write.
                    writer.readyEvent().join();

                    // If false is returned, then readyEvent must be waited for before trying again.
                    boolean accepted = writer.write(List.of(
                            new TableRow("one", "один"),
                            new TableRow("two", "два"))
                    );

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
            TableReader<TableRow> reader = client.readTable(
                    ReadTable.<TableRow>builder()
                            .setPath(table)
                            .setSerializationContext(new SerializationContext<>(TableRow.class))
                            .build()).join();

            List<TableRow> rows = new ArrayList<>();

            try {
                // We will read while we can.
                while (reader.canRead()) {
                    // We wait until we can continue reading.
                    reader.readyEvent().join();

                    List<TableRow> currentRows;
                    while ((currentRows = reader.read()) != null) {
                        rows.addAll(currentRows);
                    }
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to read");
            } finally {
                reader.close().join();
            }

            for (TableRow row : rows) {
                System.out.println("russian: " + row.russian + "; english: " + row.english);
            }
        }
    }
}
