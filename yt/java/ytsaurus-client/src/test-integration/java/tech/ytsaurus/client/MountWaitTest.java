package tech.ytsaurus.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
        yt = ytFixture.getYt();
    }

    @Test
    public void createMountAndWait() {
        yt.waitProxies().join();
        waitTabletCells(yt);

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
    public void waitProxiesMultithreaded() throws InterruptedException, ExecutionException {
        final int threads = 20;
        CountDownLatch startedWaits = new CountDownLatch(threads);
        List<Callable<Void>> tasks = new ArrayList<>(threads);

        for (int i = 0; i < threads; i++) {
            tasks.add(() -> {
                CompletableFuture<Void> waitProxiesFuture = yt.waitProxies();
                startedWaits.countDown();
                startedWaits.await();
                waitProxiesFuture.get();
                return null;
            });
        }

        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        try {
            for (var result : executorService.invokeAll(tasks, 60, TimeUnit.SECONDS)) {
                result.get();
            }
        } finally {
            executorService.shutdownNow();
            Assert.assertTrue("Worker threads did not terminate",
                    executorService.awaitTermination(60, TimeUnit.SECONDS));
        }
    }
}
