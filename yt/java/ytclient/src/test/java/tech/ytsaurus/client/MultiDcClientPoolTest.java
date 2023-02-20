package tech.ytsaurus.client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.junit.Before;
import org.junit.Test;
import tech.ytsaurus.client.rpc.DataCenterMetricsHolder;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientPool;
import tech.ytsaurus.lang.NonNullApi;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static ru.yandex.yt.testlib.FutureUtils.getError;
import static ru.yandex.yt.testlib.FutureUtils.waitFuture;

@SuppressWarnings("DuplicatedCode")
public class MultiDcClientPoolTest {
    ExecutorService executorService;
    MockRpcClientFactory mockRpcClientFactory;
    Metrics metrics;

    @Before
    public void before() {
        executorService = Executors.newFixedThreadPool(1);
        mockRpcClientFactory = new MockRpcClientFactory();
        metrics = new Metrics();
    }

    @Test
    public void testLocalDcAvailable() {
        ClientPool localDcPool = newClientPool("local");
        ClientPool remoteDcPool = newClientPool("remote");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .setLocalDc("local")
                .addClientPool(localDcPool)
                .addClientPool(remoteDcPool)
                .setDcMetricHolder(metrics)
                .build();

        var updateDoneFuture = localDcPool.updateClients(List.of(HostPort.parse("local:1")));
        waitFuture(updateDoneFuture, 100);

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isDone(), is(true));
            assertThat(clientFuture.join().getAddressString(), is("local:1"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testRemoteDcAvailable() {
        ClientPool localDcPool = newClientPool("local");
        ClientPool remoteDcPool = newClientPool("remote");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .setLocalDc("local")
                .addClientPool(localDcPool)
                .addClientPool(remoteDcPool)
                .setDcMetricHolder(metrics)
                .build();

        var updateDoneFuture = remoteDcPool.updateClients(List.of(HostPort.parse("remote:1")));
        waitFuture(updateDoneFuture, 100);

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isDone(), is(true));
            assertThat(clientFuture.join().getAddressString(), is("remote:1"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testMultipleRemoteDcAvailable() {
        ClientPool remote1DcPool = newClientPool("remote1");
        ClientPool remote2DcPool = newClientPool("remote2");
        ClientPool remote3DcPool = newClientPool("remote3");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .addClientPool(remote1DcPool)
                .addClientPool(remote2DcPool)
                .addClientPool(remote3DcPool)
                .setDcMetricHolder(metrics)
                .build();

        var updateDoneFuture = remote1DcPool.updateClients(List.of(HostPort.parse("remote1:1")));
        waitFuture(updateDoneFuture, 100);

        updateDoneFuture = remote2DcPool.updateClients(List.of(HostPort.parse("remote2:1")));
        waitFuture(updateDoneFuture, 100);

        updateDoneFuture = remote3DcPool.updateClients(List.of(HostPort.parse("remote3:1")));
        waitFuture(updateDoneFuture, 100);

        metrics.setPing("remote1", 12);
        metrics.setPing("remote2", 8);
        metrics.setPing("remote3", 15);

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isDone(), is(true));
            assertThat(clientFuture.join().getAddressString(), is("remote2:1"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testNoProxyAvailableAtTheMoment() {
        ClientPool remote1DcPool = newClientPool("remote1");
        ClientPool remote2DcPool = newClientPool("remote2");
        ClientPool remote3DcPool = newClientPool("remote3");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .addClientPool(remote1DcPool)
                .addClientPool(remote2DcPool)
                .addClientPool(remote3DcPool)
                .setDcMetricHolder(metrics)
                .build();

        metrics.setPing("remote1", 12);
        metrics.setPing("remote2", 8);
        metrics.setPing("remote3", 15);

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isDone(), is(false));

            var updateFuture = remote3DcPool.updateClients(List.of(HostPort.parse("remote3:1")));
            waitFuture(updateFuture, 100);

            assertThat(clientFuture.isDone(), is(true));
            assertThat(clientFuture.join().getAddressString(), is("remote3:1"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testErrorLocalDc() {
        DataCenterRpcClientPool localDcPool = ErroneousClientPool.immediateErrors("local");
        ClientPool remote1DcPool = newClientPool("remote1");
        ClientPool remote2DcPool = newClientPool("remote2");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .setLocalDc("local")
                .addClientPool(localDcPool)
                .addClientPool(remote1DcPool)
                .addClientPool(remote2DcPool)
                .setDcMetricHolder(metrics)
                .build();

        var updateDoneFuture = remote1DcPool.updateClients(List.of(HostPort.parse("remote:1")));
        waitFuture(updateDoneFuture, 100);

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isDone(), is(true));
            assertThat(clientFuture.join().getAddressString(), is("remote:1"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testErrorLocalDcAndRemoteDc() {
        DataCenterRpcClientPool localDcPool = ErroneousClientPool.immediateErrors("local");
        DataCenterRpcClientPool remote1DcPool = ErroneousClientPool.immediateErrors("remote1");
        ClientPool remote2DcPool = newClientPool("remote2");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .setLocalDc("local")
                .addClientPool(localDcPool)
                .addClientPool(remote1DcPool)
                .addClientPool(remote2DcPool)
                .setDcMetricHolder(metrics)
                .build();

        var updateDoneFuture = remote2DcPool.updateClients(List.of(HostPort.parse("remote2:1")));
        waitFuture(updateDoneFuture, 100);

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isDone(), is(true));
            assertThat(clientFuture.join().getAddressString(), is("remote2:1"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testAllErrors() {
        DataCenterRpcClientPool localDcPool = ErroneousClientPool.immediateErrors("local");
        DataCenterRpcClientPool remote1DcPool = ErroneousClientPool.immediateErrors("remote1");
        DataCenterRpcClientPool remote2DcPool = ErroneousClientPool.immediateErrors("remote2");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .setLocalDc("local")
                .addClientPool(localDcPool)
                .addClientPool(remote1DcPool)
                .addClientPool(remote2DcPool)
                .setDcMetricHolder(metrics)
                .build();

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isCompletedExceptionally(), is(true));
            assertThat(getError(clientFuture).getMessage(), containsString("no clients today"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testAllErrorsDelayed() throws InterruptedException {
        ErroneousClientPool localDcPool = ErroneousClientPool.delayedErrors("local");
        ErroneousClientPool remote1DcPool = ErroneousClientPool.delayedErrors("remote1");
        ErroneousClientPool remote2DcPool = ErroneousClientPool.delayedErrors("remote2");

        RpcClientPool clientPool = MultiDcClientPool.builder()
                .setLocalDc("local")
                .addClientPool(localDcPool)
                .addClientPool(remote1DcPool)
                .addClientPool(remote2DcPool)
                .setDcMetricHolder(metrics)
                .build();

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture = clientPool.peekClient(done);
            assertThat(clientFuture.isDone(), is(false));

            remote1DcPool.completeExceptionally();
            Thread.sleep(100);
            assertThat(clientFuture.isDone(), is(false));

            remote2DcPool.completeExceptionally();
            Thread.sleep(100);
            assertThat(clientFuture.isDone(), is(false));

            localDcPool.completeExceptionally();
            waitFuture(clientFuture, 100);
            assertThat(clientFuture.isCompletedExceptionally(), is(true));
            assertThat(getError(clientFuture).getMessage(), containsString("no clients today"));
        } finally {
            done.complete(null);
        }
    }

    ClientPool newClientPool(String dcName) {
        return new ClientPool(
                dcName,
                5,
                mockRpcClientFactory,
                executorService,
                new Random());
    }

    static class Metrics implements DataCenterMetricsHolder {
        Map<String, Double> metrics = new HashMap<>();

        @Override
        public double getDc99thPercentile(String dc) {
            return metrics.getOrDefault(dc, 1000.0);
        }

        void setPing(String dc, double ping) {
            metrics.put(dc, ping);
        }
    }

    @NonNullApi
    static class ErroneousClientPool implements DataCenterRpcClientPool {
        final String dataCenterName;
        final CompletableFuture<RpcClient> future = new CompletableFuture<>();

        ErroneousClientPool(String dataCenterName) {
            this.dataCenterName = dataCenterName;
        }

        static ErroneousClientPool immediateErrors(String dataCenterName) {
            return new ErroneousClientPool(dataCenterName).completeExceptionally();
        }

        static ErroneousClientPool delayedErrors(String dataCenterName) {
            return new ErroneousClientPool(dataCenterName);
        }

        public ErroneousClientPool completeExceptionally() {
            future.completeExceptionally(new Exception("no clients today"));
            return this;
        }

        @Override
        public CompletableFuture<RpcClient> peekClient(
                CompletableFuture<?> releaseFuture,
                Predicate<RpcClient> filter
        ) {
            // We don't return future here, because client can cancel it
            // and we don't want our source future to be prematurely canceled.
            return future.thenApply(client -> client);
        }

        @Override
        public String getDataCenterName() {
            return dataCenterName;
        }

        @Override
        public CompletableFuture<Integer> banClient(String address) {
            throw new RuntimeException("not implemented");
        }

    }
}
