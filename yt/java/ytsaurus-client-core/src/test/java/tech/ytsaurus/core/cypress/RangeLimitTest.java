package tech.ytsaurus.core.cypress;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeNode;

public class RangeLimitTest {
    @Test
    public void fromTree() {
        List<YTreeNode> key1 = List.of(YTree.integerNode(1));
        KeyBound keyBound = new KeyBound(key1);
        List<YTreeNode> key2 = List.of(YTree.stringNode("first"), YTree.doubleNode(0.1));
        RangeLimit rangeLimit = new RangeLimit(key2, keyBound, 10, -1, 1);
        YTreeNode node = rangeLimit.toTree(YTree.builder()).build();
        Assert.assertEquals(RangeLimit.fromTree(node), rangeLimit);
    }
}
