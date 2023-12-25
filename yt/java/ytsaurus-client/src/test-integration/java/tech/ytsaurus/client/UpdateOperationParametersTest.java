package tech.ytsaurus.client;

import org.junit.Test;
import tech.ytsaurus.client.request.UpdateOperationParameters;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

import static org.junit.Assert.assertEquals;

public class UpdateOperationParametersTest {
    final GUID operation = GUID.valueOf("aa901c3f-4cb91cb1-3fe03e8-19cdeeff");
    final UpdateOperationParameters updateOperationParameters = UpdateOperationParameters.builder()
            .setOperationId(operation)
            .setOwners("owner1", "owner2")
            .setPool("pool1")
            .addSchedulingOptions("physical",
                    new UpdateOperationParameters.SchedulingOptions()
                            .setWeight(0.33)
                            .setResourceLimits(
                                    new UpdateOperationParameters.ResourceLimits()
                                            .setCpu(0.66)
                                            .setMemory(10)
                                            .setNetwork(20)
                                            .setUserSlots(4)
                            )
            )
            .addSchedulingOptions("cloud",
                    new UpdateOperationParameters.SchedulingOptions().setResourceLimits(
                            new UpdateOperationParameters.ResourceLimits(16L, null, null, null)
                    )
            )
            .setAnnotations(
                    YTree.mapBuilder().key("description").value("New description").buildMap()
            )
            .build();

    @Test
    public void testToYTreeNode() {
        YTreeNode expected = YTree.mapBuilder()
                .key("owners").value(
                        YTree.listBuilder().value("owner1").value("owner2").buildList()
                )
                .key("pool").value("pool1")
                .key("scheduling_options_per_pool_tree").value(
                        YTree.mapBuilder()
                                .key("physical").value(
                                        YTree.mapBuilder()
                                                .key("weight").value(0.33)
                                                .key("resource_limits").value(
                                                        YTree.mapBuilder()
                                                                .key("cpu").value(0.66)
                                                                .key("memory").value(10)
                                                                .key("network").value(20)
                                                                .key("user_slots").value(4)
                                                                .buildMap()
                                                )
                                                .buildMap()
                                )
                                .key("cloud").value(
                                        YTree.mapBuilder()
                                                .key("resource_limits").value(
                                                        YTree.mapBuilder()
                                                                .key("user_slots").value(16)
                                                                .buildMap()
                                                )
                                                .buildMap()
                                )
                                .buildMap()
                )
                .key("annotations").value(
                        YTree.mapBuilder().key("description").value("New description").buildMap()
                )
                .buildMap();
        assertEquals(expected, updateOperationParameters.toTreeParametersOnly());
    }
}
