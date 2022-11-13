package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import org.junit.Test;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.YtTimestamp;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class ApiServiceTransactionTest {
    private static class ApiServiceClientStub extends ApiServiceClientImpl {
        private int pingCount = 0;

        public ApiServiceClientStub() {
            super(null, new RpcOptions(), ForkJoinPool.commonPool(), null, YandexSerializationResolver.getInstance());
        }

        @Override
        public CompletableFuture<Void> pingTransaction(GUID id) {
            pingCount++;
            return CompletableFuture.completedFuture(null);
        }

        public int getPingCount() {
            return pingCount;
        }
    }

    @Test
    public void testPingStopping() throws InterruptedException {
        var client = new ApiServiceClientStub();
        var duration = Duration.ofMillis(100);
        var transaction = new ApiServiceTransaction(
                client,
                GUID.create(),
                YtTimestamp.NULL,
                true,
                false,
                false,
                duration,
                Executors.newSingleThreadScheduledExecutor());

        Thread.sleep(duration.toMillis() * 3);
        int beforeStopping = client.getPingCount();
        assertThat(beforeStopping, greaterThan(0));

        transaction.stopPing();
        Thread.sleep(duration.toMillis() * 3);
        int justAfterStopping = client.getPingCount();
        assertThat(justAfterStopping - beforeStopping, lessThan(3));

        Thread.sleep(duration.toMillis() * 3);
        int laterAfterStopping = client.getPingCount();
        assertThat(laterAfterStopping, equalTo(justAfterStopping));
    }
}
