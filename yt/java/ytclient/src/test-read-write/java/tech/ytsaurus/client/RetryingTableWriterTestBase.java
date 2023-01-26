package tech.ytsaurus.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequestsTestingController;
import tech.ytsaurus.client.rpc.TestingOptions;
import tech.ytsaurus.core.cypress.YPath;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializerFactory;
import ru.yandex.yt.ytclient.proxy.request.ReadTable;

public class RetryingTableWriterTestBase extends YtClientTestBase {
    protected int defaultFutureTimeoutSeconds = 2000;

    protected Supplier<RetryPolicy> getRetryPolicy() {
        return () -> RetryPolicy.attemptLimited(3, RetryPolicy.forCodes(100));
    }

    protected YtFixture createYtFixtureWithOutageController(OutageController outageController) {
        var rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setOutageController(outageController)
                .setRpcRequestsTestingController(rpcRequestsTestingController);

        RpcOptions rpcOptions = new RpcOptions()
                .setTestingOptions(testingOptions)
                .setRetryPolicyFactory(getRetryPolicy())
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO);

        return createYtFixture(rpcOptions);
    }

    protected List<TableRow> readTable(YPath tablePath, YtClient yt) throws Exception {
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
    protected static class TableRow implements Comparable<TableRow> {
        private String field;

        public TableRow(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
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
