package tech.ytsaurus.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import javax.persistence.Entity;

import tech.ytsaurus.client.AsyncReader;
import tech.ytsaurus.client.AsyncWriter;
import tech.ytsaurus.client.YTsaurusClient;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;

public class ExampleReadWriteEntity {
    private ExampleReadWriteEntity() {
    }

    @Entity
    static class TableRow {
        private String english;
        private String russian;

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

            // Create table

            TableSchema tableSchema = TableSchema.builder()
                    .addValue("english", ColumnValueType.STRING)
                    .addValue("russian", ColumnValueType.STRING)
                    .build().toWrite();

            client.createNode(CreateNode.builder()
                    .setPath(table)
                    .setType(CypressNodeType.TABLE)
                    .setAttributes(Map.of("schema", tableSchema.toYTree()))
                    .setIgnoreExisting(true)
                    .build()
            ).join();

            // Write a table.

            // Create the writer.
            AsyncWriter<TableRow> writer = client.writeTableV2(new WriteTable<>(table, TableRow.class)).join();

            writer.write(List.of(
                    new TableRow("one", "один"),
                    new TableRow("two", "два"))
            ).join();

            writer.finish().join();

            // Read a table.

            // Create the reader.
            AsyncReader<TableRow> reader = client.readTableV2(new ReadTable<>(table, TableRow.class)).join();

            List<TableRow> rows = new ArrayList<>();
            var executor = Executors.newSingleThreadExecutor();
            // Read all rows asynchronously.
            reader.acceptAllAsync(rows::add, executor).join();

            for (TableRow row : rows) {
                System.out.println("russian: " + row.russian + "; english: " + row.english);
            }
        }
    }
}
