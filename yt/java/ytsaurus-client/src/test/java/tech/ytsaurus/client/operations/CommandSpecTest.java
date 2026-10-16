package tech.ytsaurus.client.operations;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.MockYTsaurusClient;
import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class CommandSpecTest {
    private final MockYTsaurusClient client = new MockYTsaurusClient("test");

    @Test
    public void testDiskRequest() {
        DiskRequest diskRequest = DiskRequest.builder()
                .setDiskSpace(DataSize.fromGigaBytes(3))
                .setInodeCount(2000L)
                .setAccount("tmp")
                .setMediumName("ssd")
                .build();

        CommandSpec spec = CommandSpec.builder()
                .setCommand("cat")
                .setDiskRequest(diskRequest)
                .build();

        Map<String, YTreeNode> node = spec.prepare(
                YTree.builder(),
                client,
                new SpecPreparationContext(YTsaurusClientConfig.builder().build())
        ).build().mapNode().asMap();

        Assert.assertEquals(diskRequest.getDiskSpace().get().toBytes(),
                node.get("disk_request").asMap().get("disk_space").longValue());
        Assert.assertEquals(diskRequest.getInodeCount().get().longValue(),
                node.get("disk_request").asMap().get("inode_count").longValue());
        Assert.assertEquals(diskRequest.getAccount().get(), node.get("disk_request").asMap().get("account")
                .stringValue());
        Assert.assertEquals(diskRequest.getMediumName().get(), node.get("disk_request").asMap().get("medium_name")
                .stringValue());
    }

    @Test
    public void testDiskRequestRequiresMediumNameWithAccount() {
        IllegalStateException exception = Assert.assertThrows(
                IllegalStateException.class,
                () -> DiskRequest.builder()
                        .setDiskSpace(DataSize.fromGigaBytes(3))
                        .setAccount("tmp")
                        .build()
        );

        Assert.assertEquals("\"medium_name\" is required in disk request if \"account\" is specified",
                exception.getMessage());
    }

    @Test
    public void testDiskRequestRequiresDiskSpace() {
        IllegalStateException exception = Assert.assertThrows(
                IllegalStateException.class,
                () -> DiskRequest.builder()
                        .setMediumName("ssd")
                        .build()
        );

        Assert.assertEquals("\"disk_space\" is required in disk request", exception.getMessage());
    }
}
