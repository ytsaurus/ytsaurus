package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.request.SerializationContext;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rpc.RpcError;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.request.ExistsNode;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RetryingTableWriterV2Test extends RetryingTableWriterTestBase {
    @Test
    public void testCorrectnessV2() throws Exception {
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

            var writer = writeTableV2(yt, tablePath, maxWritesInFlight, chunkSize);

            try {
                int partSize = curData.size() / partsCount;
                for (int partId = 0; partId < partsCount; ++partId) {
                    int from = (curData.size() * partId) / partsCount;
                    int to = from + partSize;
                    writer.write(curData.subList(from, to)).join();
                }
            } finally {
                writer.finish().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
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
            writer = writeTableV2(yt, tablePath.append(true), maxWritesInFlight, chunkSize);

            try {
                writer.write(curData).join();
            } finally {
                writer.finish().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
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
    public void testRetryErrorsV2() throws Exception {
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

            var writer = writeTableV2(yt, tablePath, maxWritesInFlight, chunkSize);

            try {
                int partSize = data.size() / partsCount;
                for (int partId = 0; partId < partsCount; ++partId) {

                    int from = (data.size() * partId) / partsCount;
                    int to = from + partSize;

                    try {
                        writer.write(data.subList(from, to)).join();
                    } catch (Throwable ex) {
                        if (done) {
                            throw ex;
                        }
                        break;
                    }
                }
            } finally {
                try {
                    writer.finish().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
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
    public void testDifferentInitFailsV2() throws Exception {
        var outageController = new OutageController();
        var ytFixture = createYtFixtureWithOutageController(outageController);
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

            var writer = writeTableV2(yt, tablePath.append(true));
            try {
                writer.write(data).join();
                assertThat("Shouldn't have got here", false);
            } catch (Exception ex) {
                // StartTransaction fail
            }

            outageController.clear();
        }

        // LockNode fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-4");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            outageController.addFails("LockNode", 1, error);

            var writer = writeTableV2(yt, tablePath.append(true));
            try {
                writer.write(data).join();
                assertThat("Shouldn't have got here", false);
            } catch (Exception ex) {
            }

            outageController.clear();
        }

        // Local transaction start fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-5");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            var writer = writeTableV2(yt, tablePath.append(true));

            outageController.addFails("StartTransaction", 1, error);

            try {
                writer.write(data).join();
                writer.finish().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (Exception ex) {
            }

            outageController.clear();
        }

        // Commit transaction fail.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-6");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            var writer = writeTableV2(yt, tablePath.append(true), 1, 10);

            outageController.addOk("CommitTransaction", 1);
            outageController.addFails("CommitTransaction", 1, error);

            writer.write(data).join();

            try {
                writer.finish().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (Exception ex) {
            }

            outageController.clear();
        }

        // CreateNode fail, we try to create node because append=false.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-2");

            outageController.addFails("CreateNode", 1, error);

            var writer = writeTableV2(yt, tablePath);
            try {
                writer.write(data).join();
                assertThat("Shouldn't have got here", false);
            } catch (Exception ex) {
                // StartTransaction fail
            }

            outageController.clear();
        }

        // CreateNode fail, but it isn't affect because append=true and no need to create node.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-3");
            yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

            outageController.addFails("CreateNode", 1, error);

            var writer = writeTableV2(yt, tablePath.append(true));

            try {
                writer.write(List.of(new TableRow(""))).join();
            } catch (Exception ex) {
                assertThat("Shouldn't have got here", false);
            }

            try {
                writer.finish().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            } catch (Exception ex) {
                assertThat("Shouldn't have got here", false);
            }
            outageController.clear();
        }
    }

    private AsyncWriter<TableRow> writeTableV2(TransactionalClient yt,
                                               YPath tablePath,
                                               int maxWritesInFlight,
                                               int chunkSize) throws Exception {
        return yt.writeTableV2(WriteTable.<TableRow>builder()
                .setPath(tablePath)
                .setSerializationContext(new SerializationContext<>(YTreeObjectSerializerFactory.forClass(TableRow.class)))
                .setNeedRetries(true)
                .setMaxWritesInFlight(maxWritesInFlight)
                .setChunkSize(chunkSize)
                .build()
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
    }

    private AsyncWriter<TableRow> writeTableV2(YtClient yt, YPath tablePath) throws Exception {
        return yt.writeTableV2(WriteTable.<TableRow>builder()
                .setPath(tablePath)
                .setSerializationContext(new SerializationContext<>(YTreeObjectSerializerFactory.forClass(TableRow.class)))
                .setNeedRetries(true)
                .build()
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
    }
}
