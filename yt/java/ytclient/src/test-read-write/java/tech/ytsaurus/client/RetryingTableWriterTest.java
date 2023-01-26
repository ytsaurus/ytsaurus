package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rpc.RpcError;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RetryingTableWriterTest extends RetryingTableWriterTestBase {
    @Test
    public void testCorrectness() throws Exception {
        var ytFixture = createYtFixture();
        var yt = ytFixture.yt;

        // Create some test data.
        List<TableRow> data = new ArrayList<>();
        for (int i = 0; i != 10000; ++i) {
            data.add(new TableRow(Integer.toString(i)));
        }

        Object[][] testCases = new Object[][]{
                /* dataSize, maxWritesInFlight, chunkSize, partsCount, existsTable */
                {10000, 1, 100, 100, false},
                {10000, 3, 100, 100, false},
                {10000, 1, 1000, 100, false},
                {1000, 1, 1000, 20, false},
                {1000, 5, 1000, 20, false},
                {1000, 1, 1000, 20, true},
        };

        int caseId = 0;
        for (Object[] testCase : testCases) {
            var tablePath = ytFixture.testDirectory.child("static-table-" + caseId);
            ++caseId;

            int dataSize = (int) testCase[0];
            int maxWritesInFlight = (int) testCase[1];
            int chunkSize = (int) testCase[2];
            int partsCount = (int) testCase[3];
            boolean existsTable = (boolean) testCase[4];

            List<TableRow> curData = data.subList(0, dataSize);

            if (existsTable) {
                yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);
            }

            var writer = writeTable(yt, tablePath, maxWritesInFlight, chunkSize);

            try {
                int partSize = curData.size() / partsCount;
                for (int partId = 0; partId < partsCount; ++partId) {
                    writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

                    int from = (curData.size() * partId) / partsCount;
                    int to = from + partSize;

                    boolean written = writer.write(curData.subList(from, to));
                    assertThat(written, is(true));
                }
            } finally {
                writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            }

            List<TableRow> result = readTable(tablePath, yt);
            if (maxWritesInFlight != 1) {
                TableRow[] sortedResult = new TableRow[result.size()];
                Arrays.sort(result.toArray(sortedResult));

                TableRow[] sortedData = new TableRow[curData.size()];
                Arrays.sort(curData.toArray(sortedData));

                assertThat(sortedResult, is(sortedData));
            } else {
                assertThat(result, is(curData));
            }

            // Try to add the same data with append=true
            writer = writeTable(yt, tablePath.append(true), maxWritesInFlight, chunkSize);

            try {
                writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                boolean written = writer.write(curData);
                assertThat(written, is(true));
            } finally {
                writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            }

            result = readTable(tablePath, yt);
            if (maxWritesInFlight != 1) {
                TableRow[] sortedResult = new TableRow[result.size()];
                Arrays.sort(result.toArray(sortedResult));

                List<TableRow> expectedData = data.subList(0, curData.size());
                expectedData.addAll(curData);

                TableRow[] sortedData = new TableRow[expectedData.size()];
                Arrays.sort(expectedData.toArray(sortedData));

                assertThat(sortedResult, is(sortedData));
            } else {
                List<TableRow> expectedData = data.subList(0, curData.size());
                expectedData.addAll(curData);

                assertThat(result, is(expectedData));
            }
        }
    }

    @Test
    public void testRetryErrors() throws Exception {
        var outageController = new OutageController();
        var ytFixture = createYtFixtureWithOutageController(outageController);
        var yt = ytFixture.yt;

        // Create some test data.
        List<TableRow> data = new ArrayList<>();
        for (int i = 0; i != 10000; ++i) {
            data.add(new TableRow(Integer.toString(i)));
        }

        int maxWritesInFlight = 1;
        int chunkSize = 1000;

        var error100 = new RpcError(
                TError.newBuilder().setCode(100).build()
        );

        var error150 = new RpcError(
                TError.newBuilder().setCode(150).build()
        );

        Object[][] testCases = new Object[][]{
                /* partsCount, failsCount, error, done */
                {1, 1, error100, true},
                {1, 1, error150, false},
                {1, 2, error100, true},
                {1, 3, error100, false},
                {5, 1, error100, true},
                {5, 3, error100, false},
                {100, 1, error100, true},
                {100, 3, error100, false},
                {1000, 3, error100, false},
                {1000, 1, error100, true},
        };


        int caseId = 0;
        for (Object[] testCase : testCases) {
            caseId++;
            var tablePath = ytFixture.testDirectory.child("static-table-" + caseId);

            int partsCount = (int) testCase[0];
            int failsCount = (int) testCase[1];
            Throwable error = (Throwable) testCase[2];
            boolean done = (boolean) testCase[3];

            outageController.addFails("WriteTable", failsCount, error);

            var writer = writeTable(yt, tablePath, maxWritesInFlight, chunkSize);

            try {
                int partSize = data.size() / partsCount;
                for (int partId = 0; partId < partsCount; ++partId) {
                    try {
                        writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                    } catch (Throwable ex) {
                        if (done) {
                            throw ex;
                        }
                        break;
                    }

                    int from = (data.size() * partId) / partsCount;
                    int to = from + partSize;

                    boolean written = writer.write(data.subList(from, to));

                    if (done) {
                        assertThat(written, is(true));
                    }
                }
            } finally {
                try {
                    CompletableFuture<?> f = writer.close();
                    f.get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                    if (!done) {
                        assertThat("Shouldn't have got here", false);
                    }
                } catch (Throwable ex) {
                    if (done) {
                        throw ex;
                    }
                }
            }

            if (done) {
                List<TableRow> result = readTable(tablePath, yt);
                assertThat(result, is(data));
            } else {
                assertThat(yt.existsNode(new ExistsNode(tablePath)).join(), is(false));
            }

            outageController.clear();
        }
    }

    @Test
    public void testSchema() throws Exception {
        var outageController = new OutageController();
        var ytFixture = createYtFixtureWithOutageController(outageController);
        var yt = ytFixture.yt;

        // Table exists, append=true, OK.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-1");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            var writer = writeTable(yt, tablePath.append(true));

            TableSchema schema = writer.getTableSchema().get(2, TimeUnit.SECONDS);
            assertThat(schema.isUniqueKeys(), is(false));
            assertThat(schema.isStrict(), is(false));

            writer.close().get(2, TimeUnit.SECONDS);
        }

        // Table exists, append=false, OK.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-2");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            var writer = writeTable(yt, tablePath);

            TableSchema schema = writer.getTableSchema().get(2, TimeUnit.SECONDS);
            assertThat(schema.isUniqueKeys(), is(false));
            assertThat(schema.isStrict(), is(false));

            writer.close().get(2, TimeUnit.SECONDS);
        }

        // Table doesn't exist, append=false, OK.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-3");

            var writer = writeTable(yt, tablePath);

            TableSchema schema = writer.getTableSchema().get(2, TimeUnit.SECONDS);
            assertThat(schema.isUniqueKeys(), is(false));
            assertThat(schema.isStrict(), is(false));

            writer.close().get(2, TimeUnit.SECONDS);
        }

        // Table doesn't exist, append=true, FAIL.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-4");

            try {
                writeTable(yt, tablePath.append(true));
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
            }
        }
    }

    @Test
    public void testDifferentInitFails() throws Exception {
        var outageController = new OutageController();
        var ytFixture = createYtFixtureWithOutageController(outageController);
        var yt = ytFixture.yt;

        var error = new RpcError(
                TError.newBuilder().setCode(199).build()
        );

        // Create some test data.
        List<TableRow> data = new ArrayList<>();
        for (int i = 0; i != 10; ++i) {
            data.add(new TableRow(Integer.toString(i)));
        }

        // Main StartTransaction fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-1");

            outageController.addFails("StartTransaction", 1, error);

            try {
                writeTable(yt, tablePath.append(true));
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
                // StartTransaction fail
            }

            outageController.clear();
        }

        // LockNode fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-4");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            outageController.addFails("LockNode", 1, error);

            try {
                writeTable(yt, tablePath.append(true));
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
            }

            outageController.clear();
        }

        // Local transaction start fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-5");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            var writer = writeTable(yt, tablePath.append(true));

            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

            outageController.addFails("StartTransaction", 1, error);

            writer.write(data);

            try {
                writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
            }

            outageController.clear();
        }

        // Commit transaction fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-6");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            var writer = writeTable(yt, tablePath.append(true), 1, 10);

            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

            outageController.addOk("CommitTransaction", 1);
            outageController.addFails("CommitTransaction", 1, error);

            writer.write(data);

            try {
                writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
            }

            outageController.clear();
        }

        // CreateNode fail, we try to create node because append=false.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-2");

            outageController.addFails("CreateNode", 1, error);

            try {
                writeTable(yt, tablePath);
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
                // StartTransaction fail
            }

            outageController.clear();
        }

        // CreateNode fail, but it isn't affect because append=true and no need to create node.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-3");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            outageController.addFails("CreateNode", 1, error);

            var writer = writeTable(yt, tablePath.append(true));

            try {
                writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            } catch (ExecutionException ex) {
                assertThat("Shouldn't have got here", false);
            }

            try {
                writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            } catch (ExecutionException ex) {
                assertThat("Shouldn't have got here", false);
            }
            outageController.clear();
        }
    }

    @Test
    public void testWriteInTransaction() throws Exception {
        var outageController = new OutageController();
        var ytFixture = createYtFixtureWithOutageController(outageController);
        var yt = ytFixture.yt;

        // Create some test data.
        List<TableRow> data = new ArrayList<>();
        for (int i = 0; i != 1000; ++i) {
            data.add(new TableRow(Integer.toString(i)));
        }

        int maxWritesInFlight = 1;
        int chunkSize = 1000;

        var error100 = new RpcError(
                TError.newBuilder().setCode(100).build()
        );

        Object[][] testCases = new Object[][]{
                /* partsCount, failsCount, error, abortParentTransaction, done */
                {1, 0, null, false, true},
                {1, 0, null, true, false},
                {1, 1, error100, false, true},
                {1, 1, error100, true, false},
                {1, 3, error100, false, false},
                {1, 3, error100, true, false},

                {5, 0, null, false, true},
                {5, 0, null, true, false},
                {5, 3, error100, false, false},
                {5, 3, error100, true, false},

                {100, 0, null, false, true},
                {100, 0, null, true, false},

                {1000, 0, null, false, true},
                {1000, 0, null, true, false}
        };


        int caseId = 0;
        for (Object[] testCase : testCases) {
            caseId++;
            var tablePath = ytFixture.testDirectory.child("static-table-" + caseId);

            int partsCount = (int) testCase[0];
            int failsCount = (int) testCase[1];
            Throwable error = (Throwable) testCase[2];
            boolean abortParentTransaction = (boolean) testCase[3];
            boolean done = (boolean) testCase[4];


            outageController.addFails("WriteTable", failsCount, error);

            var parentTransaction = yt.startTransaction(new StartTransaction(TransactionType.Master)).join();

            var writer = writeTable(parentTransaction, tablePath, maxWritesInFlight, chunkSize);

            try {
                int partSize = data.size() / partsCount;
                for (int partId = 0; partId < partsCount; ++partId) {
                    try {
                        writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                    } catch (Throwable ex) {
                        if (done) {
                            throw ex;
                        }
                        break;
                    }

                    int from = (data.size() * partId) / partsCount;
                    int to = from + partSize;

                    boolean written = writer.write(data.subList(from, to));

                    if (done || abortParentTransaction) {
                        assertThat(written, is(true));
                    }
                }
            } finally {
                try {
                    CompletableFuture<?> f = writer.close();
                    f.get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                    if (!done && !abortParentTransaction) {
                        assertThat("Shouldn't have got here", false);
                    }
                } catch (Throwable ex) {
                    if (done) {
                        throw ex;
                    }
                }
            }

            assertThat(yt.existsNode(new ExistsNode(tablePath)).join(), is(false));

            if (abortParentTransaction) {
                parentTransaction.abort();
                assertThat(yt.existsNode(new ExistsNode(tablePath)).join(), is(false));
            }

            if (done) {
                parentTransaction.commit().join();
                List<TableRow> result = readTable(tablePath, yt);
                assertThat(result, is(data));
            }

            outageController.clear();
        }
    }

    private TableWriter<TableRow> writeTable(TransactionalClient yt,
                                             YPath tablePath,
                                             int maxWritesInFlight,
                                             int chunkSize) throws Exception {
        return yt.writeTable(WriteTable.<TableRow>builder()
                .setPath(tablePath)
                .setSerializationContext(new SerializationContext<>(
                        YTreeObjectSerializerFactory.forClass(TableRow.class)))
                .setNeedRetries(true)
                .setMaxWritesInFlight(maxWritesInFlight)
                .setChunkSize(chunkSize)
                .build()
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
    }

    private TableWriter<TableRow> writeTable(YtClient yt, YPath tablePath) throws Exception {
        return yt.writeTable(WriteTable.<TableRow>builder()
                .setPath(tablePath)
                .setSerializationContext(new SerializationContext<>(
                        YTreeObjectSerializerFactory.forClass(TableRow.class)))
                .setNeedRetries(true)
                .build()
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
    }
}
