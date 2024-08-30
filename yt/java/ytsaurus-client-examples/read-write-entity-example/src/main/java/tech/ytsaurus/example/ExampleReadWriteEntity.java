package tech.ytsaurus.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import javax.persistence.Entity;

import tech.ytsaurus.client.AsyncReader;
import tech.ytsaurus.client.AsyncWriter;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.ReadTable;
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
        // You need to set up cluster address in YT_PROXY environment variable.
        var clusterAddress = System.getenv("YT_PROXY");
        if (clusterAddress == null || clusterAddress.isEmpty()) {
            throw new IllegalArgumentException("Environment variable YT_PROXY is empty");
        }

        YTsaurusClient client = YTsaurusClient.builder()
                .setCluster(clusterAddress)
                .build();

        try (client) {
            // The table is located in `//tmp` and contains the name of the current user.
            // The username is necessary in case two people run this example at the same time
            // so that they use different output tables.
            YPath table = YPath.simple("//tmp/" + System.getProperty("user.name") + "-read-write");

            // Write a table.

            // Create the writer.
            // The table with the inferred from TableRow schema will be created by writeTableV2.
            AsyncWriter<TableRow> writer = client.writeTableV2(new WriteTable<>(table, TableRow.class)).join();

            writer.write(List.of(
                    new TableRow("one", "один"),
                    new TableRow("two", "два"))
            ).join();

            writer.finish().join();

            // Read a table.

            // Create the reader.
            try (AsyncReader<TableRow> reader = client.readTableV2(new ReadTable<>(table, TableRow.class)).join()) {
                List<TableRow> rows = new ArrayList<>();
                var executor = Executors.newSingleThreadExecutor();
                // Read all rows asynchronously.
                reader.acceptAllAsync(rows::add, executor).join();

                for (TableRow row : rows) {
                    System.out.println("russian: " + row.russian + "; english: " + row.english);
                }

                executor.shutdown();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
