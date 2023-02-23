package tech.ytsaurus.client;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.rpc.RpcError;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.typeinfo.TiType;

import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeKeyField;
import ru.yandex.inside.yt.kosher.impl.ytree.object.annotation.YTreeObject;
import ru.yandex.inside.yt.kosher.impl.ytree.object.serializers.YTreeObjectSerializer;
import ru.yandex.yt.testlib.ExceptionUtils;
import ru.yandex.yt.testlib.Matchers;
import ru.yandex.yt.ytclient.proxy.MappedModifyRowsRequest;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.MountTable;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class YtClientRetryWithTransactionTest extends YtClientTestBase {
    static final TableSchema keyValueTableSchema = TableSchema.builder()
            .setUniqueKeys(true)
            .addKey("key", TiType.string())
            .addValue("value", TiType.int64())
            .build();
    ExecutorService executor = Executors.newSingleThreadExecutor();

    RpcOptions options;

    @Before
    public void before() {
        options = new RpcOptions();
        options.setMinBackoffTime(Duration.ZERO);
        options.setMaxBackoffTime(Duration.ZERO);

        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void after() {
        executor.shutdown();
    }

    @Test
    public void testSimple() {
        var ytFixture = createYtFixture(options);
        var tablePath = ytFixture.testDirectory.child("table");
        var yt = ytFixture.yt;

        var serializer = new YTreeObjectSerializer<>(KeyValue.class);

        yt.createNode(
                new CreateNode(tablePath, CypressNodeType.TABLE)
                        .addAttribute("schema", keyValueTableSchema.toYTree())
                        .addAttribute("dynamic", true)
        ).join();
        yt.mountTableAndWaitTablets(new MountTable(tablePath.toString())).join();

        Function<String, Integer> readKey = (String key) -> {
            var res = yt.lookupRows(
                    new LookupRowsRequest(tablePath.toString(), keyValueTableSchema.toLookup())
                            .addFilter(key),
                    serializer
            ).join();
            if (res.size() != 1) {
                throw new RuntimeException("Expected exactly on result, got: " + res);
            }
            return res.get(0).value;
        };

        yt.retryWithTabletTransaction(
                tx -> {
                    var modifyRows = new MappedModifyRowsRequest<>(tablePath.toString(), serializer);
                    modifyRows
                            .addInsert(new KeyValue("foo", 0));

                    return tx.modifyRows(modifyRows);
                },
                executor,
                RetryPolicy.noRetries()
        ).join();

        var failingAction1 = new FailingIncrementKeyAction(tablePath, "foo", 4, () -> rpcError(100));
        yt.retryWithTabletTransaction(
                failingAction1,
                executor,
                RetryPolicy.attemptLimited(5, RetryPolicy.forCodes(100))
        ).join();
        assertThat(failingAction1.attempts, is(5));
        assertThat(readKey.apply("foo"), is(1));

        var failingAction2 = new FailingIncrementKeyAction(tablePath, "foo", 5, () -> rpcError(424242));
        var fut2 = yt.retryWithTabletTransaction(
                failingAction2,
                executor,
                RetryPolicy.attemptLimited(5, RetryPolicy.forCodes(424242))
        );
        var exception2 = assertThrows(Throwable.class, fut2::join);
        assertThat(failingAction2.attempts, is(5));
        assertThat(exception2, Matchers.isCausedBy(RpcError.class));
        assertThat(ExceptionUtils.getCause(exception2, RpcError.class).getError().getCode(), is(424242));
    }

    @Test(timeout = 20000)
    public void testBlockingUserCall() {
        var ytFixture = createYtFixture(options);
        var yt = ytFixture.yt;

        AtomicInteger counter = new AtomicInteger(0);
        final int threadCount = 10;
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        for (int i = 0; i != 10; ++i) {
            CompletableFuture<Void> currentFuture = yt.retryWithTabletTransaction(
                    tx -> {
                        counter.incrementAndGet();
                        //noinspection StatementWithEmptyBody
                        while (counter.get() != threadCount) {
                        }
                        return CompletableFuture.completedFuture(null);
                    },
                    ForkJoinPool.commonPool(),
                    RetryPolicy.noRetries()
            );
            futureList.add(currentFuture);
        }
        CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0])).join();
    }

    @Test()
    public void testAbortedTransaction() {
        var ytFixture = createYtFixture(options);
        var yt = ytFixture.yt;

        yt.retryWithTabletTransaction(
                tx -> {
                    tx.close();
                    return CompletableFuture.completedFuture(null);
                },
                executor,
                RetryPolicy.noRetries()
        ).join();
    }

    static RpcError rpcError(int code) {
        return new RpcError(
                TError.newBuilder()
                        .setCode(code)
                        .setMessage("error")
                        .build());
    }

    @YTreeObject
    static class KeyValue {
        @YTreeKeyField
        String key;

        int value;

        public KeyValue(String key, int value) {
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
            return value == keyValue.value && Objects.equals(key, keyValue.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, value);
        }

        @Override
        public String toString() {
            return "KeyValue{" +
                    "key='" + key + '\'' +
                    ", value=" + value +
                    '}';
        }
    }

    static class FailingIncrementKeyAction implements Function<ApiServiceTransaction, CompletableFuture<Void>> {
        final YPath tablePath;
        final String key;
        private final Supplier<Throwable> errorSupplier;
        int remainingFailures;
        int attempts = 0;

        FailingIncrementKeyAction(
                YPath tablePath,
                String key,
                int failingAttemptCount,
                Supplier<Throwable> errorSupplier
        ) {
            this.tablePath = tablePath;
            this.key = key;
            this.errorSupplier = errorSupplier;
            this.remainingFailures = failingAttemptCount;
        }

        @Override
        public CompletableFuture<Void> apply(ApiServiceTransaction tx) {
            attempts++;

            var serializer = new YTreeObjectSerializer<>(KeyValue.class);
            var res = tx.lookupRows(
                    new LookupRowsRequest(tablePath.toString(), keyValueTableSchema.toLookup())
                            .addFilter(key),
                    serializer
            ).thenAccept(keyValues -> {
                if (keyValues.size() != 1) {
                    throw new RuntimeException("Values: " + keyValues);
                }
                var current = keyValues.get(0);
                var modifyRows = new MappedModifyRowsRequest<>(tablePath.toString(), serializer);
                modifyRows
                        .addInsert(new KeyValue(key, current.value + 1));
                tx.modifyRows(modifyRows);
            });

            if (remainingFailures > 0) {
                remainingFailures--;
                throw new RuntimeException(errorSupplier.get());
            }

            return res;
        }
    }
}
