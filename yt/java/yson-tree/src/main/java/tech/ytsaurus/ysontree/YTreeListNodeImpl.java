package tech.ytsaurus.ysontree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

public class YTreeListNodeImpl extends YTreeNodeImpl implements YTreeListNode {

    private final List<YTreeNode> list = new ArrayList<>();

    public YTreeListNodeImpl(@Nullable Map<String, YTreeNode> attributes) {
        super(attributes);
    }

    @Override
    public int size() {
        return list.size();
    }

    @Override
    public void clear() {
        list.clear();
    }

    @Override
    public YTreeNode get(int index) {
        return list.get(index);
    }

    @Override
    public YTreeNode set(int index, YTreeNode value) {
        return list.set(index, value);
    }

    @Override
    public YTreeNode remove(int index) {
        return list.remove(index);
    }

    @Override
    public void add(YTreeNode value) {
        list.add(value);
    }

    @Override
    public void addAll(List<YTreeNode> values) {
        list.addAll(values);
    }

    @Override
    public Iterator<YTreeNode> iterator() {
        return list.iterator();
    }

    @Override
    public List<YTreeNode> asList() {
        return list;
    }

    @Override
    public int hashCode() {
        return hashCodeBase() * 4243 + list.hashCode();
    }

    @Override
    public boolean equals(Object another) {
        if (this == another) {
            return true;
        }
        if (another == null || !(another instanceof YTreeListNode)) {
            return false;
        }
        YTreeListNode node = (YTreeListNode) another;
        return list.equals(node.asList()) && equalsBase(node);
    }

}
