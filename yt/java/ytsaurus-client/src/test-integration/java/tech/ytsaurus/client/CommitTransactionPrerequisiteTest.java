package tech.ytsaurus.client;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.CompletionException;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.LookupRowsRequest;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.PrerequisiteOptions;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.rows.UnversionedRowset;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.common.YTsaurusErrorCode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTree;

import static org.junit.Assert.assertThrows;

public class CommitTransactionPrerequisiteTest extends YTsaurusClientTestBase {

    private static final TableSchema SCHEMA = new TableSchema.Builder()
            .addKey("key", ColumnValueType.STRING)
            .addValue("value", ColumnValueType.STRING)
            .build();

    @Test
    public void commitWithAlivePrerequisiteSucceeds() {
        YTsaurusFixture ytFixture = createYtFixture();
        YTsaurusClient yt = ytFixture.getYt();
        YPath table = ytFixture.getTestDirectory().child("table");
        prepareDynTable(yt, table);

        try (ApiServiceTransaction master = yt.startTransaction(StartTransaction.master()).join();
             ApiServiceTransaction tx = yt.startTransaction(StartTransaction.tablet()).join()) {
            tx.modifyRows(insertRow(table, "k1", "v1")).join();
            tx.commit(builder -> builder.setPrerequisiteOptions(
                    new PrerequisiteOptions().setTransactionsIds(master.getId())
            )).join();
        }

        Assert.assertEquals(1, lookupKey(yt, table, "k1").getRows().size());
    }

    @Test
    public void commitWithAbortedPrerequisiteFails() {
        YTsaurusFixture ytFixture = createYtFixture();
        YTsaurusClient yt = ytFixture.getYt();
        YPath table = ytFixture.getTestDirectory().child("table");
        prepareDynTable(yt, table);

        ApiServiceTransaction master = yt.startTransaction(StartTransaction.master()).join();
        GUID masterId = master.getId();
        master.abort().join();

        try (ApiServiceTransaction tx = yt.startTransaction(StartTransaction.tablet()).join()) {
            tx.modifyRows(insertRow(table, "k1", "v1")).join();
            CompletionException error = assertThrows(
                    CompletionException.class,
                    () -> tx.commit(builder -> builder.setPrerequisiteOptions(
                            new PrerequisiteOptions().setTransactionsIds(masterId)
                    )).join()
            );
            Throwable cause = error.getCause();
            Assert.assertTrue(cause instanceof YTsaurusError);
            YTsaurusError ytError = (YTsaurusError) cause;
            Assert.assertTrue(ytError.matches(YTsaurusErrorCode.PrerequisiteCheckFailed.getCode()));
        }

        Assert.assertEquals(0, lookupKey(yt, table, "k1").getRows().size());
    }

    private void prepareDynTable(YTsaurusClient yt, YPath path) {
        waitTabletCells(yt);

        yt.createNode(CreateNode.builder()
                .setPath(path)
                .setType(CypressNodeType.TABLE)
                .setAttributes(Map.of(
                        "dynamic", YTree.booleanNode(true),
                        "schema", SCHEMA.toYTree()
                ))
                .build()).join();

        yt.mountTableAndWaitTablets(MountTable.builder().setPath(path.toString()).build()).join();
    }

    private static ModifyRowsRequest insertRow(YPath path, String key, String value) {
        return ModifyRowsRequest.builder()
                .setPath(path.toString())
                .setSchema(SCHEMA)
                .addInsert(Arrays.asList(key, value))
                .build();
    }

    private static UnversionedRowset lookupKey(YTsaurusClient yt, YPath path, String key) {
        return yt.lookupRows(
                LookupRowsRequest.builder()
                        .setPath(path.toString())
                        .setSchema(SCHEMA.toLookup())
                        .addFilter(key)
                        .build()
        ).join();
    }

}
