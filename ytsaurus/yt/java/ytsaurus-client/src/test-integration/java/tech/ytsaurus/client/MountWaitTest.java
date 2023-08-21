package tech.ytsaurus.client;

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
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


public class MountWaitTest extends YTsaurusClientTestBase {
    private YTsaurusClient yt;

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

        yt.createNode(
                CreateNode.builder()
                        .setPath(YPath.simple(path))
                        .setType(CypressNodeType.TABLE)
                        .setAttributes(attributes)
                        .build()
        ).join();

        CompletableFuture<Void> mountFuture = yt.mountTable(path, null, false, true);

        mountFuture.join();
        var tablets = yt.getNode(path + "/@tablets").join().asList();
        boolean allTabletsReady = true;
        for (YTreeNode tablet : tablets) {
            if (!tablet.mapNode().getOrThrow("state").stringValue().equals("mounted")) {
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
        executorService.awaitTermination(60, TimeUnit.SECONDS);
        Assert.assertEquals(startedWaits.get(), joinedThreads.get());
    }
}
