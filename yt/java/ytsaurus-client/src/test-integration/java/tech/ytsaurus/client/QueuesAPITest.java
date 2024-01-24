package tech.ytsaurus.client;

import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ModifyRowsRequest;
import tech.ytsaurus.client.request.MountTable;
import tech.ytsaurus.client.request.PullConsumer;
import tech.ytsaurus.client.request.RegisterQueueConsumer;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.rows.QueueRowset;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnSchema;
import tech.ytsaurus.core.tables.ColumnSortOrder;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

public class QueuesAPITest extends YTsaurusClientTestBase {
    private YTsaurusFixture ytFixture;

    @Before
    public void setup() {
        this.ytFixture = createYtFixture();
        ytFixture.yt.waitProxies().join();
        waitTabletCells(ytFixture.yt);
    }

    @Test
    public void testPullConsumer() {
        YTsaurusClient yt = ytFixture.yt;

        YPath queuePath = ytFixture.testDirectory.child("queue");
        YPath consumerPath = ytFixture.testDirectory.child("consumer");

        createAndFillQueue(yt, queuePath);
        createConsumer(yt, consumerPath);

        yt.registerQueueConsumer(
                RegisterQueueConsumer.builder()
                        .setQueuePath(queuePath)
                        .setConsumerPath(consumerPath)
                        .build()
        ).join();

        QueueRowset rowset = yt.pullConsumer(
                PullConsumer.builder()
                        .setQueuePath(queuePath)
                        .setConsumerPath(consumerPath)
                        .setPartitionIndex(0)
                        .build()
        ).join();

        Assert.assertEquals(3, rowset.getRows().size());
    }

    private static void createAndFillQueue(YTsaurusClient yt, YPath queuePath) {
        TableSchema queueSchema = TableSchema.builder()
                .addValue("data", ColumnValueType.STRING)
                .addValue("$timestamp", ColumnValueType.UINT64)
                .addValue("$cumulative_data_weight", ColumnValueType.INT64)
                .build();

        var queueAttributes = new HashMap<String, YTreeNode>();

        queueAttributes.put("dynamic", new YTreeBuilder().value(true).build());
        queueAttributes.put("schema", queueSchema.toYTree());

        yt.createNode(
                CreateNode.builder()
                        .setPath(queuePath)
                        .setType(CypressNodeType.TABLE)
                        .setAttributes(queueAttributes)
                        .build()
        ).join();

        yt.mountTableAndWaitTablets(MountTable.builder().setPath(queuePath).build()).join();

        try (ApiServiceTransaction tx = yt.startTransaction(StartTransaction.tablet()).join()) {
            tx.modifyRows(
                    ModifyRowsRequest.builder()
                            .setPath(queuePath.toString())
                            .setSchema(TableSchema.builder()
                                    .addValue("data", ColumnValueType.STRING)
                                    .build())
                            .addInsert(List.of("foo"))
                            .addInsert(List.of("bar"))
                            .addInsert(List.of("foobar"))
                            .build()
            ).join();

            tx.commit().join();
        }
    }

    private static void createConsumer(YTsaurusClient yt, YPath consumerPath) {
        TableSchema consumerSchema = TableSchema.builder()
                .add(
                        ColumnSchema.builder("queue_cluster", ColumnValueType.STRING, true)
                                .setSortOrder(ColumnSortOrder.ASCENDING)
                                .build()
                )
                .add(
                        ColumnSchema.builder("queue_path", ColumnValueType.STRING, true)
                                .setSortOrder(ColumnSortOrder.ASCENDING)
                                .build()
                )
                .add(
                        ColumnSchema.builder("partition_index", ColumnValueType.UINT64, true)
                                .setSortOrder(ColumnSortOrder.ASCENDING)
                                .build()
                )
                .add(
                        ColumnSchema.builder("offset", ColumnValueType.UINT64, true).build()
                )
                .build();

        var queueAttributes = new HashMap<String, YTreeNode>();

        queueAttributes.put("dynamic", new YTreeBuilder().value(true).build());
        queueAttributes.put("treat_as_queue_consumer", new YTreeBuilder().value(true).build());
        queueAttributes.put("schema", consumerSchema.toYTree());

        yt.createNode(
                CreateNode.builder()
                        .setPath(consumerPath)
                        .setType(CypressNodeType.TABLE)
                        .setAttributes(queueAttributes)
                        .build()
        ).join();

        yt.mountTableAndWaitTablets(MountTable.builder().setPath(consumerPath).build()).join();
    }
}
