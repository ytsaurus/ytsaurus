package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.Test;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.TError;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.StartTransaction;
import ru.yandex.yt.ytclient.proxy.request.TransactionType;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.rpc.RpcError;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequestsTestingController;
import ru.yandex.yt.ytclient.rpc.TestingOptions;
import ru.yandex.yt.ytclient.tables.TableSchema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RetryingTableWriterTest extends YtClientTestBase {
    int defaultFutureTimeoutSeconds = 2000;

    private Supplier<RetryPolicy> getRetryPolicy() {
        return () -> RetryPolicy.attemptLimited(3, RetryPolicy.forCodes(100));
    }

    @Test
    public void testCorrectness() throws Exception {
        var ytFixture = createYtFixture();
        var yt = ytFixture.yt;

        // Create some test data.
        List<TableRow> data = new ArrayList<>();
        for (int i = 0; i != 10000; ++i) {
            data.add(new TableRow(Integer.toString(i)));
        }

        Object[][] testCases = new Object[][] {
                /* dataSize, maxWritesInFlight, chunkSize, partsCount, existsTable */
                { 10000, 1, 100, 100, false },
                { 10000, 3, 100, 100, false },
                { 10000, 1, 1000, 100, false },
                { 1000, 1, 1000, 20, false },
                { 1000, 5, 1000, 20, false },
                { 1000, 1, 1000, 20, true },
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
                yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);
            }

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
                            .setMaxWritesInFlight(maxWritesInFlight)
                            .setChunkSize(chunkSize)
            ).join();

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
            writer = yt.writeTable(
                    new WriteTable<>(tablePath.append(true), YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
                            .setMaxWritesInFlight(maxWritesInFlight)
                            .setChunkSize(chunkSize)
            ).join();

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
        var rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setOutageController(outageController)
                .setRpcRequestsTestingController(rpcRequestsTestingController);

        RpcOptions rpcOptions = new RpcOptions()
                .setTestingOptions(testingOptions)
                .setRetryPolicyFactory(getRetryPolicy())
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO);

        var ytFixture = createYtFixture(rpcOptions);
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

        Object[][] testCases = new Object[][] {
                /* partsCount, failsCount, error, done */
                { 1, 1, error100, true },
                { 1, 1, error150, false },
                { 1, 2, error100, true },
                { 1, 3, error100, false },
                { 5, 1, error100, true },
                { 5, 3, error100, false },
                { 100, 1, error100, true },
                { 100, 3, error100, false },
                { 1000, 3, error100, false },
                { 1000, 1, error100, true },
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

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
                            .setMaxWritesInFlight(maxWritesInFlight)
                            .setChunkSize(chunkSize)
            ).join();

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
        var rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setOutageController(outageController)
                .setRpcRequestsTestingController(rpcRequestsTestingController);

        var ytFixture = createYtFixture(new RpcOptions().setTestingOptions(testingOptions));
        var yt = ytFixture.yt;

        // Table exists, append=true, OK.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-1");
            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath.append(true), YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
            ).join();

            TableSchema schema = writer.getTableSchema().get(2, TimeUnit.SECONDS);
            assertThat(schema.isUniqueKeys(), is(false));
            assertThat(schema.isStrict(), is(false));

            writer.close().get(2, TimeUnit.SECONDS);
        }

        // Table exists, append=false, OK.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-2");
            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
            ).join();

            TableSchema schema = writer.getTableSchema().get(2, TimeUnit.SECONDS);
            assertThat(schema.isUniqueKeys(), is(false));
            assertThat(schema.isStrict(), is(false));

            writer.close().get(2, TimeUnit.SECONDS);
        }

        // Table doesn't exist, append=false, OK.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-3");

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
            ).join();

            TableSchema schema = writer.getTableSchema().get(2, TimeUnit.SECONDS);
            assertThat(schema.isUniqueKeys(), is(false));
            assertThat(schema.isStrict(), is(false));

            writer.close().get(2, TimeUnit.SECONDS);
        }

        // Table doesn't exist, append=true, FAIL.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-4");

            try {
                yt.writeTable(
                        new WriteTable<>(tablePath.append(true), YTreeObjectSerializerFactory.forClass(TableRow.class))
                                .setNeedRetries(true)
                ).join();
                assertThat("Shouldn't have got here", false);
            } catch (CompletionException ex) {
            }
        }
    }

    @Test
    public void testDifferentInitFails() throws Exception {
        var outageController = new OutageController();
        var rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setOutageController(outageController)
                .setRpcRequestsTestingController(rpcRequestsTestingController);

        RpcOptions rpcOptions = new RpcOptions()
                .setTestingOptions(testingOptions)
                .setRetryPolicyFactory(getRetryPolicy())
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO);

        var ytFixture = createYtFixture(rpcOptions);
        var yt = ytFixture.yt;

        var error = new RpcError(
                TError.newBuilder().setCode(199).build()
        );

        // Create some test data.
        List<TableRow> data = new ArrayList<>();
        for (int i = 0; i != 100; ++i) {
            data.add(new TableRow(Integer.toString(i)));
        }

        // Main StartTransaction fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-1");

            outageController.addFails("StartTransaction", 1, error);

            try {
                yt.writeTable(new WriteTable<>(
                        tablePath.append(true),
                        YTreeObjectSerializerFactory.forClass(TableRow.class)).setNeedRetries(true)
                ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
                // StartTransaction fail
            }

            outageController.clear();
        }

        // LockNode fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-4");
            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            outageController.addFails("LockNode", 1, error);

            try {
                yt.writeTable(
                        new WriteTable<>(tablePath.append(true), YTreeObjectSerializerFactory.forClass(TableRow.class))
                                .setNeedRetries(true)
                ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
            }

            outageController.clear();
        }

        // Local transaction start fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-5");
            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath.append(true), YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
            ).join();

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
            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath.append(true), YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
                            .setChunkSize(10)
            ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

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
                yt.writeTable(
                        new WriteTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
                                .setNeedRetries(true)
                ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
                // StartTransaction fail
            }

            outageController.clear();
        }

        // CreateNode fail, but it isn't affect because append=true and no need to create node.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-3");
            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            outageController.addFails("CreateNode", 1, error);

            var writer = yt.writeTable(
                    new WriteTable<>(tablePath.append(true), YTreeObjectSerializerFactory.forClass(TableRow.class))
                            .setNeedRetries(true)
            ).join();

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
        var ytFixture = createYtFixture();
        var yt = ytFixture.yt;

        var tablePath = ytFixture.testDirectory.child("static-table");

        try {
            yt.startTransaction(new StartTransaction(TransactionType.Master)).thenCompose(
                    tx -> tx.writeTable(
                            new WriteTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
                                    .setNeedRetries(true))).join();
            assertThat("Exception was expected", false);
        } catch (CompletionException ex) {
            assertThat("Expected IllegalStateException", ex.getCause() instanceof IllegalStateException);
        }
    }

    private List<TableRow> readTable(YPath tablePath, YtClient yt) throws Exception {
        List<TableRow> result = new ArrayList<>();
        var reader = yt.readTable(
                new ReadTable<>(tablePath, YTreeObjectSerializerFactory.forClass(TableRow.class))
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        // Read data and make sure it is what we have written.
        try {
            while (reader.canRead()) {
                reader.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                List<TableRow> cur;
                while ((cur = reader.read()) != null) {
                    result.addAll(cur);
                }
                reader.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            }
        } finally {
            reader.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            reader.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

        return result;
    }

    @YTreeObject
    static class TableRow implements Comparable<TableRow> {
        public String field;

        public TableRow(String field) {
            this.field = field;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TableRow)) {
                return false;
            }
            TableRow other = (TableRow) obj;
            return Objects.equals(field, other.field);
        }

        @Override
        public int compareTo(TableRow other) {
            return field.compareTo(other.field);
        }

        @Override
        public String toString() {
            return String.format("TableRow(\"%s\")", field);
        }
    }
}
