package tech.ytsaurus.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.persistence.Entity;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.request.ReadFile;
import tech.ytsaurus.client.request.ReadTable;
import tech.ytsaurus.client.request.SetNode;
import tech.ytsaurus.client.request.WriteFile;
import tech.ytsaurus.client.request.WriteTable;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.TestingOptions;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;

public class CypressTest extends YTsaurusClientTestBase {
    int defaultFutureTimeoutSeconds = 2000;

    @Test
    public void testOutageExistsNode() throws Exception {
        OutageController outageController = new OutageController();
        TestingOptions testingOptions = new TestingOptions().setOutageController(outageController);

        var ytFixture = createYtFixture(new RpcOptions().setTestingOptions(testingOptions));
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        yt.existsNode(tablePath.toString()).get(2, TimeUnit.SECONDS);

        outageController.addFails("ExistsNode", 2, new RuntimeException("some error"));
        outageController.addFails("OtherQuery", 2, new RuntimeException("some error"));

        int errorsCount = 0;
        for (int retryId = 0; retryId < 3; ++retryId) {
            try {
                yt.existsNode(tablePath.toString()).get(2, TimeUnit.SECONDS);
            } catch (Exception ex) {
                ++errorsCount;
            }
        }

        yt.existsNode(tablePath.toString()).get(2, TimeUnit.SECONDS);

        Assert.assertEquals(2, errorsCount);
    }

    @Test
    public void testDelayedExistsNode() throws Exception {
        OutageController outageController = new OutageController();
        TestingOptions testingOptions = new TestingOptions().setOutageController(outageController);

        var ytFixture = createYtFixture(new RpcOptions().setTestingOptions(testingOptions));
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        yt.existsNode(tablePath.toString()).get(2, TimeUnit.SECONDS);

        outageController.addDelays("ExistsNode", 2, Duration.ofMillis(500));
        outageController.addDelays("OtherQuery", 2, Duration.ofSeconds(3));

        for (int retryId = 0; retryId < 2; ++retryId) {
            long start = System.currentTimeMillis();
            yt.existsNode(tablePath.toString()).join();
            Assert.assertTrue("Has delay=500ms", System.currentTimeMillis() - start > 500);
        }

        long start = System.currentTimeMillis();
        yt.existsNode(tablePath.toString()).join();
        Assert.assertTrue("No delay", System.currentTimeMillis() - start < 400);
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    @Test(timeout = 1000000)
    public void testBanningProxyReadingWritingTable() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        // Create some test data.
        List<TableRow> data = new ArrayList<>();
        for (int i = 0; i != 100000; ++i) {
            data.add(new TableRow(Integer.toString(i)));
        }

        //
        // Write table and while we write data ban the proxy.
        // We expect connection not to be closed until writer completes.
        yt.createNode(tablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

        var writer = yt.writeTable(
                new WriteTable<>(tablePath, TableRow.class)
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        try {
            // Write first half of the data.
            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            boolean written = writer.write(data.subList(0, data.size() / 2));
            Assert.assertTrue(written);

            // Ban the proxy.
            Assert.assertTrue(writer instanceof TableWriterImpl);
            String proxyAddress = ((TableWriterImpl<?>) writer).getRpcProxyAddress();
            yt.banProxy(proxyAddress).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

            // Write second half of the data.
            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            written = writer.write(data.subList(data.size() / 2, data.size()));
            Assert.assertTrue(written);
        } finally {
            writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

        // Now create reader
        List<TableRow> result = new ArrayList<>();
        var reader = yt.readTable(
                new ReadTable<>(tablePath, TableRow.class)
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        // Immediately ban proxy that reader is connected to.
        {
            Assert.assertTrue(reader instanceof TableReaderImpl);
            String proxyAddress = ((TableReaderImpl<?>) reader).getRpcProxyAddress();
            yt.banProxy(proxyAddress).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

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

        Assert.assertEquals(data, result);
    }

    @SuppressWarnings("checkstyle:AvoidNestedBlocks")
    @Test(timeout = 1000000)
    public void testBanningProxyReadingWritingFile() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        yt.createNode(tablePath.toString(), CypressNodeType.FILE).get(2, TimeUnit.SECONDS);
        var writer = yt.writeFile(
                new WriteFile(tablePath.toString())
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        final byte[] aData = new byte[512 * 1024];
        Arrays.fill(aData, (byte) 'a');
        final byte[] bData = new byte[512 * 1024];
        Arrays.fill(bData, (byte) 'a');

        try {
            // Write first half of the data.
            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            boolean written = writer.write(aData);
            Assert.assertTrue(written);

            // Ban the proxy.
            Assert.assertTrue(writer instanceof FileWriterImpl);
            String proxyAddress = ((FileWriterImpl) writer).getRpcProxyAddress();
            yt.banProxy(proxyAddress).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

            // Write second half of the data.
            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            written = writer.write(bData);
            Assert.assertTrue(written);
        } finally {
            writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

        // Now create reader
        var reader = yt.readFile(
                new ReadFile(tablePath.toString())
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        // Immediately ban proxy that reader is connected to.
        {
            Assert.assertTrue(reader instanceof FileReaderImpl);
            String proxyAddress = ((FileReaderImpl) reader).getRpcProxyAddress();
            yt.banProxy(proxyAddress).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

        List<byte[]> result = new ArrayList<>();
        try {
            while (reader.canRead()) {
                reader.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
                byte[] cur;
                while ((cur = reader.read()) != null) {
                    result.add(cur);
                }
                reader.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            }
        } finally {
            reader.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            reader.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

        final byte[] expectedData = new byte[aData.length + bData.length];
        System.arraycopy(expectedData, 0, aData, 0, aData.length);
        System.arraycopy(expectedData, aData.length, bData, 0, bData.length);

        final byte[] actualData = new byte[result.stream().mapToInt(b -> b.length).sum()];
        {
            int pos = 0;
            for (var cur : result) {
                System.arraycopy(actualData, pos, cur, 0, cur.length);
                pos += cur.length;
            }
        }

        Assert.assertArrayEquals(expectedData, actualData);
    }

    @Test
    public void testMutationId() {
        var fixture = createYtFixture();
        var yt = fixture.yt;

        var testPath = fixture.testDirectory.child("some-list");

        yt.createNode(new CreateNode(testPath, CypressNodeType.LIST)).join();

        var setRequest = new SetNode(YPath.simple(testPath + "/end"), YTree.integerNode(1));
        yt.setNode(setRequest).join();
        Assert.assertEquals("One item", 1, yt.getNode(testPath + "/@count").join().intValue());

        yt.setNode(setRequest).join();
        Assert.assertEquals("Two items", 2, yt.getNode(testPath + "/@count").join().intValue());
    }

    @Test(timeout = 10000)
    public void testRediscoverWhenAllBanned() {
        var rpcOptions = new RpcOptions();
        rpcOptions.setProxyUpdateTimeout(Duration.ofHours(1));
        rpcOptions.setChannelPoolSize(1);

        var fixture = createYtFixture();
        var yt = fixture.yt;
        var testPath = fixture.testDirectory.child("value");
        final var expectedValue = YTree.integerNode(42);
        yt.setNode(testPath.toString(), expectedValue).join();
        var aliveDestinations = yt.getAliveDestinations();
        Assert.assertEquals(1, aliveDestinations.size());
        int totalBanned = 0;
        for (var destinationList : aliveDestinations.values()) {
            for (var destination : destinationList) {
                var proxy = ((ApiServiceClientImpl) destination).getRpcProxyAddress();
                yt.banProxy(proxy).join();
                totalBanned += 1;
            }
        }
        Assert.assertEquals(1, totalBanned);

        var actualValue = yt.getNode(testPath.toString()).join();
        Assert.assertEquals(expectedValue, actualValue);
    }

    @Entity
    static class TableRow {
        public String field;

        TableRow() {
        }

        TableRow(String field) {
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
        public int hashCode() {
            return Objects.hash(field);
        }

        @Override
        public String toString() {
            return String.format("TableRow(\"%s\")", field);
        }
    }
}
