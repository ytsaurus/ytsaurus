package tech.ytsaurus.client;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.ColumnValueType;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

public class UnmountWaitTest extends YTsaurusClientTestBase {

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

        Map<String, YTreeNode> attributes = Map.of(
                "dynamic", new YTreeBuilder().value(true).build(),
                "schema", schema.toYTree()
        );

        yt.createNode(
                CreateNode.builder()
                        .setPath(path)
                        .setType(CypressNodeType.TABLE)
                        .setAttributes(attributes)
                        .build()
        ).join();
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
