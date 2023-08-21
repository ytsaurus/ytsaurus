package tech.ytsaurus.client;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static tech.ytsaurus.testlib.FutureUtils.getError;
import static tech.ytsaurus.testlib.FutureUtils.waitFuture;
import static tech.ytsaurus.testlib.FutureUtils.waitOkResult;

class CustomException extends Exception {
    CustomException(String message) {
        super(message);
    }
}

public class ClientPoolTest extends ClientPoolTestBase {
    @Test
    public void testSimple() {
        ClientPool clientPool = newClientPool();

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {

            var clientFuture1 = clientPool.peekClient(done);
            Assert.assertFalse(clientFuture1.isDone());
            clientPool.updateClients(List.of(HostPort.parse("localhost:1")));

            Assert.assertEquals(clientFuture1.join().getAddressString(), "localhost:1");

            var clientFuture2 = clientPool.peekClient(done);
            Assert.assertTrue(clientFuture2.isDone());
            Assert.assertEquals(clientFuture2.join().getAddressString(), "localhost:1");
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testUpdateEmpty() {
        ClientPool clientPool = newClientPool();

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture1 = clientPool.peekClient(done);
            Assert.assertFalse(clientFuture1.isDone());
            clientPool.updateClients(List.of());

            waitFuture(clientFuture1, 1000);
            Assert.assertTrue(getError(clientFuture1).getMessage().contains("Cannot get rpc proxies"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testLingeringConnection() {
        ClientPool clientPool = newClientPool();

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            waitFuture(
                    clientPool.updateClients(List.of(HostPort.parse("localhost:1"))),
                    100);
            var clientFuture1 = clientPool.peekClient(done);
            Assert.assertTrue(clientFuture1.isDone());
            Assert.assertEquals(clientFuture1.join().getAddressString(), "localhost:1");
            Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:1"));

            waitFuture(clientPool.updateClients(List.of()), 100);

            Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:1"));
        } finally {
            done.complete(null);
        }
        Assert.assertFalse(mockRpcClientFactory.isConnectionOpened("localhost:1"));
    }

    @Test
    public void testCanceledConnection() {
        ClientPool clientPool = newClientPool();

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture1 = clientPool.peekClient(done);
            Assert.assertFalse(clientFuture1.isDone());
            clientFuture1.cancel(true);

            waitFuture(
                    clientPool.updateClients(List.of(HostPort.parse("localhost:1"))),
                    100);

            Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:1"));

            waitFuture(clientPool.updateClients(List.of()), 100);

            Assert.assertFalse(mockRpcClientFactory.isConnectionOpened("localhost:1"));
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testUpdateWithError() {
        ClientPool clientPool = newClientPool();

        CompletableFuture<Void> done = new CompletableFuture<>();
        try {
            var clientFuture1 = clientPool.peekClient(done);
            Assert.assertFalse(clientFuture1.isDone());

            waitFuture(clientPool.updateWithError(new CustomException("error update")), 100);
            Assert.assertTrue(getError(clientFuture1).getCause().getCause() instanceof CustomException);
        } finally {
            done.complete(null);
        }
    }

    @Test
    public void testBanUnban() {
        ClientPool clientPool = newClientPool();

        CompletableFuture<Void> done1 = new CompletableFuture<>();
        CompletableFuture<Void> done2 = new CompletableFuture<>();
        try {
            waitOkResult(
                    clientPool.updateClients(List.of(HostPort.parse("localhost:1"))),
                    100);
            var clientFuture1 = clientPool.peekClient(done1);
            waitFuture(clientFuture1, 100);
            Assert.assertEquals(clientFuture1.join().getAddressString(), "localhost:1");

            var banResult = clientPool.banErrorClient(HostPort.parse("localhost:1"));
            waitFuture(banResult, 100);

            Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:1"));

            done1.complete(null);

            Assert.assertFalse(mockRpcClientFactory.isConnectionOpened("localhost:1"));

            var clientFuture2 = clientPool.peekClient(done2);
            Assert.assertFalse(clientFuture2.isDone());

            waitOkResult(
                    clientPool.updateClients(List.of(HostPort.parse("localhost:1"))),
                    100);

            waitFuture(clientFuture2, 100);
            Assert.assertEquals(clientFuture2.join().getAddressString(), "localhost:1");
        } finally {
            done1.complete(null);
            done2.complete(null);
        }
    }

    @Test
    public void testChangedProxyList() {
        ClientPool clientPool = newClientPool();

        waitOkResult(
                clientPool.updateClients(List.of(HostPort.parse("localhost:1"))),
                100);

        Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:1"));

        waitOkResult(
                clientPool.updateClients(List.of(HostPort.parse("localhost:2"), HostPort.parse("localhost:3"))),
                100);

        Assert.assertFalse(mockRpcClientFactory.isConnectionOpened("localhost:1"));
        Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:2"));
        Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:3"));

        waitOkResult(
                clientPool.updateClients(List.of(HostPort.parse("localhost:3"), HostPort.parse("localhost:4"))),
                100);

        Assert.assertFalse(mockRpcClientFactory.isConnectionOpened("localhost:1"));
        Assert.assertFalse(mockRpcClientFactory.isConnectionOpened("localhost:2"));
        Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:3"));
        Assert.assertTrue(mockRpcClientFactory.isConnectionOpened("localhost:4"));
    }

    private static boolean tryWaitFuture(CompletableFuture<?> f) {
        try {
            f.get(100, TimeUnit.MILLISECONDS);
        } catch (ExecutionException ignored) {
        } catch (TimeoutException ignored) {
            return false;
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
        return true;
    }

    @Test
    public void testOnAllBannedCallback() throws InterruptedException, ExecutionException, TimeoutException {
        var onAllBannedCallback = new Runnable() {
            volatile CompletableFuture<Void> called = new CompletableFuture<>();
            final AtomicInteger counter = new AtomicInteger();

            @Override
            public void run() {
                counter.incrementAndGet();
                var oldCalled = called;
                called = new CompletableFuture<>();
                oldCalled.complete(null);
            }
        };

        ClientPool clientPool = newClientPool();
        clientPool.setOnAllBannedCallback(onAllBannedCallback);

        waitOkResult(
                clientPool.updateClients(
                        List.of(
                                HostPort.parse("localhost:1"),
                                HostPort.parse("localhost:2"),
                                HostPort.parse("localhost:3")
                        )),
                100);

        var f = onAllBannedCallback.called;
        Assert.assertEquals(onAllBannedCallback.counter.get(), 0);

        clientPool.banClient("localhost:1");
        Assert.assertFalse(tryWaitFuture(f));
        Assert.assertEquals(onAllBannedCallback.counter.get(), 0);

        clientPool.banClient("localhost:2");
        Assert.assertFalse(tryWaitFuture(f));
        Assert.assertEquals(onAllBannedCallback.counter.get(), 0);

        clientPool.banClient("localhost:3");
        // assert no exception
        f.get(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(onAllBannedCallback.counter.get(), 1);

        waitOkResult(
                clientPool.updateClients(
                        List.of(
                                HostPort.parse("localhost:4"),
                                HostPort.parse("localhost:5")
                        )),
                100);

        f = onAllBannedCallback.called;
        Assert.assertEquals(onAllBannedCallback.counter.get(), 1);

        clientPool.banClient("localhost:1");
        Assert.assertFalse(tryWaitFuture(f));
        Assert.assertEquals(onAllBannedCallback.counter.get(), 1);

        clientPool.banClient("localhost:4");
        Assert.assertFalse(tryWaitFuture(f));
        Assert.assertEquals(onAllBannedCallback.counter.get(), 1);

        clientPool.banClient("localhost:5");
        // assert no exception
        f.get(1000, TimeUnit.MILLISECONDS);
        Assert.assertEquals(onAllBannedCallback.counter.get(), 2);
    }

    @Test
    public void testFilter() {
        ClientPool clientPool = newClientPool();

        waitOkResult(
                clientPool.updateClients(
                        List.of(
                                HostPort.parse("localhost:1"),
                                HostPort.parse("localhost:2"),
                                HostPort.parse("localhost:3")
                        )),
                100
        );

        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        var client1 = clientPool.peekClient(
                releaseFuture,
                c -> !List.of("localhost:1", "localhost:3").contains(c.getAddressString())
        );
        Assert.assertTrue(client1.isDone());
        Assert.assertEquals(client1.join().getAddressString(), "localhost:2");

        var client2 = clientPool.peekClient(
                releaseFuture,
                c -> !List.of("localhost:1", "localhost:2", "localhost:3")
                        .contains(c.getAddressString())
        );
        Assert.assertTrue(client2.isDone());
        Assert.assertTrue(List.of("localhost:1", "localhost:2", "localhost:3")
                .contains(client2.join().getAddressString()));
    }

    @Test
    public void testWaitWhenAllBanned() {
        ClientPool clientPool = newClientPool();

        waitOkResult(
                clientPool.updateClients(
                        List.of(
                                HostPort.parse("localhost:2"),
                                HostPort.parse("localhost:3"),
                                HostPort.parse("localhost:4")
                        )),
                100
        );
        Assert.assertEquals(1, (int) clientPool.banClient("localhost:2").join());
        Assert.assertEquals(1, (int) clientPool.banClient("localhost:3").join());
        Assert.assertEquals(1, (int) clientPool.banClient("localhost:4").join());

        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        var client1 = clientPool.peekClient(releaseFuture);
        tryWaitFuture(client1);
        Assert.assertFalse(client1.isDone());
        waitOkResult(
                clientPool.updateClients(List.of(HostPort.parse("localhost:2"))),
                100
        );
        tryWaitFuture(client1);
        Assert.assertTrue(client1.isDone());
        Assert.assertEquals(client1.join().getAddressString(), "localhost:2");
    }
}

class ClientPoolTestBase {
    ExecutorService executorService;
    MockRpcClientFactory mockRpcClientFactory;

    @Before
    public void before() {
        executorService = Executors.newFixedThreadPool(1);
        mockRpcClientFactory = new MockRpcClientFactory();
    }

    @After
    public void after() {
        executorService.shutdownNow();
    }

    ClientPool newClientPool() {
        return new ClientPool(
                "testDc",
                5,
                mockRpcClientFactory,
                executorService,
                new Random());
    }

    ClientPool newClientPool(ProxySelector proxySelector) {
        return new ClientPool(
                "testDc",
                3,
                mockRpcClientFactory,
                executorService,
                new Random(),
                proxySelector);
    }
}
