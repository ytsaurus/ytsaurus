package ru.yandex.yt.ytclient.proxy;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import org.junit.Before;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.misc.reflection.ClassX;
import ru.yandex.yt.rpcproxy.ETransactionType;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;
import ru.yandex.yt.ytclient.wire.UnversionedRow;
import ru.yandex.yt.ytclient.wire.UnversionedValue;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class YtClientDynamicTableTest extends YtClientTestBase {
    static final TableSchema keyValueTableSchema = TableSchema.builder()
            .addKey("key", ColumnValueType.INT64)
            .addValue("value", ColumnValueType.STRING)
            .build();

    private YPath keyValueTablePath;
    private YtClient yt;

    static UnversionedRow createKeyValueUnversionedRow(long key, String value) {
        var values = new ArrayList<UnversionedValue>();
        values.add(new UnversionedValue(0, ColumnValueType.INT64, false, key));
        if (value == null) {
            values.add(new UnversionedValue(1, ColumnValueType.NULL, false, null));
        } else {
            var bytesValue = value.getBytes(StandardCharsets.UTF_8);
            values.add(new UnversionedValue(1, ColumnValueType.STRING, false, bytesValue));
        }
        return new UnversionedRow(values);
    }

    @Before
    public void setUpTables() {
        var ytFixture = createYtFixture();
        yt = ytFixture.yt;

        keyValueTablePath = ytFixture.testDirectory.child("key-value-table");
        yt.createNode(
                new CreateNode(keyValueTablePath, ObjectType.Table)
                        .setRecursive(true)
                        .setForce(true)
                        .setAttributes(
                                Map.of(
                                        "dynamic", YTree.booleanNode(true),
                                        "schema", keyValueTableSchema.toYTree()
                                )
                        )
        ).join();
        yt.mountTable(keyValueTablePath.toString(), null, false, true).join();
    }

    @Test(timeout = 10000)
    public void testBanningProxyHoldingTransaction() {
        // YT-13770: if proxy is banned while we are holding transaction
        // we should not release or close client until working with transaction is done
        var tx = yt.startTransaction(
                new ApiServiceTransactionOptions(ETransactionType.TT_TABLET)
                .setSticky(true)
        ).join();

        try (tx) {
            var txProxyAddress = tx.getRpcProxyAddress();
            {
                yt.banProxy(txProxyAddress).join();
            }

            tx.commit().join();
        }
    }

    @Test(timeout = 10000)
    public void testWaitForModifyRows() {
        var tx = yt.startTransaction(
                new ApiServiceTransactionOptions(ETransactionType.TT_TABLET)
                        .setSticky(true)
        ).join();
        try (tx) {
            var modifyRows =
                    new ModifyRowsRequest(keyValueTablePath.toString(), keyValueTableSchema)
                            .addInsert(Arrays.asList(2L, "two"));

            tx.modifyRows(modifyRows);
            tx.commit().join();
        }
    }

    @Test(timeout = 10000)
    public void testKeepMissingRowUnversionedRow() {
        var tx = yt.startTransaction(
                new ApiServiceTransactionOptions(ETransactionType.TT_TABLET)
                        .setSticky(true)
        ).join();

        try (tx) {
            var modifyRows =
                    new ModifyRowsRequest(keyValueTablePath.toString(), keyValueTableSchema)
                            .addInsert(Arrays.asList(3L, "three"))
                            .addInsert(Arrays.asList(4L, "four"))
                            .addInsert(Arrays.asList(6L, null));

            tx.modifyRows(modifyRows).join();
            tx.commit().join();
        }

        {
            var lookupRows = new LookupRowsRequest(
                    keyValueTablePath.toString(),
                    keyValueTableSchema.toLookup())
                    .addFilters(List.of(
                            List.of(1L),
                            List.of(2L),
                            List.of(3L),
                            List.of(4L),
                            List.of(5L),
                            List.of(6L)
                    ))
                    .setKeepMissingRows(true);

            var unversionedRowset = yt.lookupRows(lookupRows).join();
            assertThat(unversionedRowset.getRows(), is(
                    Arrays.asList(
                            null,
                            null,
                            createKeyValueUnversionedRow(3, "three"),
                            createKeyValueUnversionedRow(4, "four"),
                            null,
                            createKeyValueUnversionedRow(6, null)
                    )
            ));

            final var serializer = new YTreeObjectSerializer<>(ClassX.wrap(KeyValue.class));
            var keyValueList = yt.lookupRows(lookupRows, serializer).join();

            assertThat(keyValueList, is(
                    Arrays.asList(
                            null,
                            null,
                            new KeyValue(3, "three"),
                            new KeyValue(4, "four"),
                            null,
                            new KeyValue(6, "") // NB. YTreeStringSerializer deserializes `#` as empty string =\
                    )
            ));
        }

    }

    @YTreeObject
    static class KeyValue {
        int key;
        String value;

        public KeyValue(int key, @Nullable String value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            KeyValue keyValue = (KeyValue) o;
            return key == keyValue.key && Objects.equals(value, keyValue.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "KeyValue{" +
                    "key=" + key +
                    ", value='" + value + '\'' +
                    '}';
        }
    }
}
