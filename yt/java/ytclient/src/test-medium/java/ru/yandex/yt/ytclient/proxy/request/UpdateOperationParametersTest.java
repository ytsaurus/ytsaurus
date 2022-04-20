package ru.yandex.yt.ytclient.proxy.request;

import junit.framework.TestCase;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters.ResourceLimits;
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters.SchedulingOptions;

public class UpdateOperationParametersTest extends TestCase  {
    final GUID OPERATION = GUID.valueOf("aa901c3f-4cb91cb1-3fe03e8-19cdeeff");
    final UpdateOperationParameters updateOperationParameters = new UpdateOperationParameters(OPERATION)
            .setOwners("owner1", "owner2")
            .setPool("pool1")
            .addSchedulingOptions("physical",
                    new SchedulingOptions().setWeight(0.33).setResourceLimits(
                            new ResourceLimits().setCpu(0.66).setMemory(10).setNetwork(20).setUserSlots(4)
                    ))
            .addSchedulingOptions("cloud",
                    new SchedulingOptions().setResourceLimits(
                            new ResourceLimits(16L, null, null, null)
                    ));

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
                .buildMap();
        assertEquals(expected, updateOperationParameters.toYTreeNode());
    }
}
