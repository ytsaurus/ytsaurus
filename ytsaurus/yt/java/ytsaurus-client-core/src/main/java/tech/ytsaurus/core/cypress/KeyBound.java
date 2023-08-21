package tech.ytsaurus.core.cypress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;
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
