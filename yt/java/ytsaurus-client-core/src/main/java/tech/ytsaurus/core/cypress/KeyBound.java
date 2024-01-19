package tech.ytsaurus.core.cypress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeListNode;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * This class represents a bound for range with list of keys and relation
 */
@NonNullApi
@NonNullFields
public class KeyBound {
    private final Relation relation;
    private final List<YTreeNode> key;

    public KeyBound(List<YTreeNode> key) {
        this(Relation.LESS, key);
    }

    public KeyBound(Relation relation, List<YTreeNode> key) {
        this.relation = relation;
        this.key = new ArrayList<>(key);
    }

    public static KeyBound of(YTreeNode... key) {
        return new KeyBound(Arrays.asList(key));
    }

    public static KeyBound of(Relation relation, YTreeNode... key) {
        return new KeyBound(relation, Arrays.asList(key));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        KeyBound keyBound = (KeyBound) o;
        return relation == keyBound.relation && key.equals(keyBound.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(relation, key);
    }

    public static KeyBound fromTree(YTreeNode node) {
        YTreeListNode listNode = node.listNode();
        Relation relation = Relation.fromValue(listNode.getString(0));
        List<YTreeNode> key = listNode.getList(1).asList();
        return new KeyBound(relation, key);
    }

    /**
     * For converting to yson representation
     */
    public YTreeBuilder toTree(YTreeBuilder builder) {
        return builder.beginList()
                .value(relation.value())
                .value(key)
                .endList();
    }
}
