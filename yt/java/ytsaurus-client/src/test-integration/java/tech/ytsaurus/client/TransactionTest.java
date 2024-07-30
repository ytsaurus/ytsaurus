package tech.ytsaurus.client;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.operations.Operation;
import tech.ytsaurus.client.operations.OperationStatus;
import tech.ytsaurus.client.operations.SortSpec;
import tech.ytsaurus.client.request.ExistsNode;
import tech.ytsaurus.client.request.SortOperation;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;

public class TransactionTest extends YTsaurusClientTestBase {

    @Test
    public void testCreateOperationOutputWithApiServiceClient() {
        var ytFixture = createYtFixture();
        var yt = ytFixture.getYt();
        var inputTable = ytFixture.getTestDirectory().child("input-table");
        var outputTable = ytFixture.getTestDirectory().child("output-table");

        createAndFillTable(yt, inputTable);

        try (ApiServiceTransaction tx = yt.startTransaction(StartTransaction.master()).join()) {
            Operation operation = yt.sort(
                    SortOperation.builder()
                            .setSpec(SortSpec.builder()
                                    .setInputTables(inputTable)
                                    .setOutputTable(outputTable)
                                    .setSortBy("name")
                                    .build())
                            .setTransactionalOptions(tx.getTransactionalOptions())
                            .build()
            ).join();
            Assert.assertEquals(OperationStatus.COMPLETED, operation.getStatus().join());
        }

        Assert.assertFalse(
                "creation of the output table must be rolled back with the transaction",
                yt.existsNode(ExistsNode.builder().setPath(outputTable).build()).join()
        );
    }

    @Test
    public void testCreateOperationOutputWithApiServiceTransaction() {
        var ytFixture = createYtFixture();
        var yt = ytFixture.getYt();
        var inputTable = ytFixture.getTestDirectory().child("input-table");
        var outputTable = ytFixture.getTestDirectory().child("output-table");

        createAndFillTable(yt, inputTable);

        try (ApiServiceTransaction tx = yt.startTransaction(StartTransaction.master()).join()) {
            Operation operation = tx.sort(
                    SortOperation.builder()
                            .setSpec(SortSpec.builder()
                                    .setInputTables(inputTable)
                                    .setOutputTable(outputTable)
                                    .setSortBy("name")
                                    .build())
                            .build()
            ).join();
            Assert.assertEquals(OperationStatus.COMPLETED, operation.getStatus().join());
        }

        Assert.assertFalse(
                "creation of the output table must be rolled back with the transaction",
                yt.existsNode(ExistsNode.builder().setPath(outputTable).build()).join()
        );
    }

    private void createAndFillTable(YTsaurusClient yt, YPath path) {
        writeTable(yt, path, createTableSchema(), List.of(
                YTree.builder().beginMap()
                        .key("name").value("a")
                        .key("count").value(1)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("c")
                        .key("count").value(3)
                        .buildMap(),
                YTree.builder().beginMap()
                        .key("name").value("b")
                        .key("count").value(2)
                        .buildMap()
        ));
    }

    private TableSchema createTableSchema() {
        TableSchema.Builder builder = new TableSchema.Builder();
        builder.setUniqueKeys(false);
        builder.addValue("name", ColumnValueType.STRING);
        builder.addValue("count", ColumnValueType.INT64);
        return builder.build().toWrite();
    }
}
