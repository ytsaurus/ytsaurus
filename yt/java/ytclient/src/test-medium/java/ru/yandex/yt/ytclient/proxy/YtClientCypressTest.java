package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.proxy.request.ReadFile;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;
import ru.yandex.yt.ytclient.proxy.request.WriteFile;
import ru.yandex.yt.ytclient.proxy.request.WriteTable;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class YtClientCypressTest extends YtClientTestBase {
    int defaultFutureTimeoutSeconds = 2000;

    @Test
    public void testOutageExistsNode() throws Exception {
        OutageController outageController = new OutageController();

        var ytFixture = createYtFixture(new RpcOptions().setOutageController(outageController));
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

        assertThat(errorsCount, is(2));
    }

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
        yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

        var writer = yt.writeTable(
                new WriteTable<>(tablePath.toString(), YTreeObjectSerializerFactory.forClass(TableRow.class))
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        try {
            // Write first half of the data.
            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            boolean written = writer.write(data.subList(0, data.size() / 2));
            assertThat(written, is(true));

            // Ban the proxy.
            assertThat(writer, instanceOf(TableWriterImpl.class));
            String proxyAddress = ((TableWriterImpl<?>) writer).getRpcProxyAddress();
            yt.banProxy(proxyAddress).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

            // Write second half of the data.
            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            written = writer.write(data.subList(data.size() / 2, data.size()));
            assertThat(written, is(true));
        } finally {
            writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

        // Now create reader
        List<TableRow> result = new ArrayList<>();
        var reader = yt.readTable(
                new ReadTable<>(tablePath.toString(), YTreeObjectSerializerFactory.forClass(TableRow.class))
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        // Immediately ban proxy that reader is connected to.
        {
            assertThat(reader, instanceOf(TableReaderImpl.class));
            String proxyAddress = ((TableReaderImpl<?>)reader).getRpcProxyAddress();
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

        assertThat(result, is(data));
    }

    @Test(timeout = 1000000)
    public void testBanningProxyReadingWritingFile() throws Exception {
        var ytFixture = createYtFixture();
        var tablePath = ytFixture.testDirectory.child("static-table");
        var yt = ytFixture.yt;

        yt.createNode(tablePath.toString(), ObjectType.File).get(2, TimeUnit.SECONDS);
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
            assertThat(written, is(true));

            // Ban the proxy.
            assertThat(writer, instanceOf(FileWriterImpl.class));
            String proxyAddress = ((FileWriterImpl) writer).getRpcProxyAddress();
            yt.banProxy(proxyAddress).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

            // Write second half of the data.
            writer.readyEvent().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
            written = writer.write(bData);
            assertThat(written, is(true));
        } finally {
            writer.close().get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);
        }

        // Now create reader
        var reader = yt.readFile(
                new ReadFile(tablePath.toString())
        ).get(defaultFutureTimeoutSeconds, TimeUnit.SECONDS);

        // Immediately ban proxy that reader is connected to.
        {
            assertThat(reader, instanceOf(FileReaderImpl.class));
            String proxyAddress = ((FileReaderImpl)reader).getRpcProxyAddress();
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

        assertThat(actualData, is(expectedData));
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
        assertThat(aliveDestinations.size(), is(1));
        int totalBanned = 0;
        for (var destinationList : aliveDestinations.values()) {
            for (var destination : destinationList) {
                var proxy = destination.getRpcProxyAddress();
                yt.banProxy(proxy).join();
                totalBanned += 1;
            }
        }
        assertThat(totalBanned, is(1));

        var actualValue = yt.getNode(testPath.toString()).join();
        assertThat(actualValue, is(expectedValue));
    }

    @YTreeObject
    static class TableRow {
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
        public String toString() {
            return String.format("TableRow(\"%s\")", field);
        }
    }
}
