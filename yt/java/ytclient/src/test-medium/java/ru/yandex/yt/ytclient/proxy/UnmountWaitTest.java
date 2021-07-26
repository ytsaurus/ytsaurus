package ru.yandex.yt.ytclient.proxy;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import ru.yandex.inside.yt.kosher.cypress.CypressNodeType;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.proxy.request.CreateNode;
import ru.yandex.yt.ytclient.tables.ColumnValueType;
import ru.yandex.yt.ytclient.tables.TableSchema;

public class UnmountWaitTest extends YtClientTestBase {

    @Test
    public void createUnmountAndWait() {
        var ytFixture = createYtFixture();
        var yt = ytFixture.yt;
        var testDirectory = ytFixture.testDirectory;

        while (!yt.getNode("//sys/tablet_cell_bundles/default/@health").join().stringValue().equals("good")) {
            try {
                //noinspection BusyWait
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        YPath path = testDirectory.child("mount-unmount-table-and-wait-test");

        TableSchema schema = new TableSchema.Builder()
                .addKey("key", ColumnValueType.STRING)
                .addValue("value", ColumnValueType.STRING)
                .build();

        Map<String, YTreeNode> attributes = ImmutableMap.of(
                "dynamic", new YTreeBuilder().value(true).build(),
                "schema", schema.toYTree()
        );

        yt.createNode(new CreateNode(path, CypressNodeType.TABLE, attributes)).join();
        yt.mountTable(path.toTree().stringValue(), null, false, true).join();
        yt.unmountTableAndWaitTablets(path.toString()).join();

        List<YTreeNode> tablets = yt.getNode(path + "/@tablets").join().asList();
        boolean allTabletsReady = true;
        for (YTreeNode tablet : tablets) {
            if (!tablet.mapNode().getOrThrow("state").stringValue().equals("unmounted")) {
                allTabletsReady = false;
                break;
            }
        }
        Assert.assertTrue(allTabletsReady);
    }
}
