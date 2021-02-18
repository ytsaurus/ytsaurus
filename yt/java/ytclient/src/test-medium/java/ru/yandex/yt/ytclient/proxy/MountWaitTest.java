package ru.yandex.yt.ytclient.proxy;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;


public class MountWaitTest extends YtClientTestBase {
    private YtClient yt;

    @Before
    public void setup() {
        var ytFixture = createYtFixture();
        yt = ytFixture.yt;
    }

    @Test
    public void createMountAndWait() {
        yt.waitProxies().join();

        while (!yt.getNode("//sys/tablet_cell_bundles/default/@health").join().stringValue().equals("good")) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        String path = "//tmp/mount-table-and-wait-test" + UUID.randomUUID().toString();

        TableSchema schema = new TableSchema.Builder()
                .addKey("key", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.STRING)
                .build();

        var attributes = new HashMap<String, YTreeNode>();

        attributes.put("dynamic", new YTreeBuilder().value(true).build());
        attributes.put("schema", schema.toYTree());

        yt.createNode(new CreateNode(path, ObjectType.Table, attributes)).join();

        CompletableFuture<Void> mountFuture = yt.mountTable(path, null, false, true);

        mountFuture.join();
        var tablets = yt.getNode(path + "/@tablets").join().asList();
        boolean allTabletsReady = true;
        for (YTreeNode tablet : tablets) {
            if (!tablet.asMap().getOrThrow("state").stringValue().equals("mounted")) {
                allTabletsReady = false;
                break;
            }
        }

        Assert.assertTrue(allTabletsReady);
    }

    @Test
    public void waitProxiesMultithreaded() throws InterruptedException {
        final int threads = 20;
        final Object startLock = new Object();

        AtomicInteger startedWaits = new AtomicInteger();
        AtomicInteger joinedThreads = new AtomicInteger();

        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; i++) {
            executorService.submit(() -> {
                CompletableFuture<Void> waitProxiesFuture;
                synchronized (startLock) {
                    waitProxiesFuture = yt.waitProxies();
                    startedWaits.getAndIncrement();
                }
                while (startedWaits.get() < threads) {
                    try {
                        synchronized (startLock) {
                            startLock.wait();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                // startedWaits == threads
                synchronized (startLock) {
                    startLock.notifyAll();
                }
                waitProxiesFuture.join();

                joinedThreads.getAndIncrement();
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(30, TimeUnit.SECONDS);
        Assert.assertEquals(startedWaits.get(), joinedThreads.get());
    }
}
