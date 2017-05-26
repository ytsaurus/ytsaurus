package ru.yandex.yt.ytclient.ytree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class YTreeBuilder implements YTreeConsumer {
    private final List<State> stack = new ArrayList<>();
    private Map<String, YTreeNode> currentAttributes;

    protected abstract static class State {
        public void onKeyedItem(String key) {
            throw new IllegalStateException("Not building a map");
        }

        public YTreeNode onEndMap() {
            throw new IllegalStateException("Not building a map");
        }

        public void onListItem() {
            throw new IllegalStateException("Not building a list");
        }

        public YTreeNode onEndList() {
            throw new IllegalStateException("Not building a list");
        }

        public void onValue(YTreeNode value) {
            throw new IllegalStateException("Not building a container");
        }

        public Map<String, YTreeNode> onEndAttributes() {
            throw new IllegalStateException("Not building attributes");
        }

        public YTreeNode result() {
            throw new IllegalStateException("Builder is not finished");
        }
    }

    protected abstract static class AbstractMapBuilder extends State {
        private final Map<String, YTreeNode> map = new LinkedHashMap<>();
        private String nextKey;

        @Override
        public void onKeyedItem(String key) {
            if (nextKey != null) {
                throw new IllegalStateException("Previous map key was not consumed");
            }
            nextKey = key;
        }

        @Override
        public void onValue(YTreeNode value) {
            if (nextKey == null) {
                throw new IllegalStateException("Previous map key is missing");
            }
            map.put(nextKey, value);
            nextKey = null;
        }

        protected Map<String, YTreeNode> finishMap() {
            if (nextKey != null) {
                throw new IllegalStateException("Previous map key was not consumed");
            }
            if (map.isEmpty()) {
                return Collections.emptyMap();
            } else {
                return Collections.unmodifiableMap(map);
            }
        }
    }

    protected static class AbstractListBuilder extends State {
        private final List<YTreeNode> list = new ArrayList<>();

        @Override
        public void onListItem() {
            // nothing
        }

        @Override
        public void onValue(YTreeNode value) {
            list.add(value);
        }

        protected List<YTreeNode> finishList() {
            if (list.isEmpty()) {
                return Collections.emptyList();
            } else {
                return Collections.unmodifiableList(list);
            }
        }
    }

    protected static class AttributesBuilder extends AbstractMapBuilder {
        @Override
        public Map<String, YTreeNode> onEndAttributes() {
            return finishMap();
        }
    }

    protected static class MapBuilder extends AbstractMapBuilder {
        private final Map<String, YTreeNode> attributes;

        public MapBuilder(Map<String, YTreeNode> attributes) {
            this.attributes = attributes;
        }

        @Override
        public YTreeNode onEndMap() {
            Map<String, YTreeNode> map = finishMap();
            if (map.isEmpty() && attributes.isEmpty()) {
                return YTreeMapNode.EMPTY;
            } else {
                return new YTreeMapNode(map, attributes);
            }
        }
    }

    protected static class ListBuilder extends AbstractListBuilder {
        private final Map<String, YTreeNode> attributes;

        public ListBuilder(Map<String, YTreeNode> attributes) {
            this.attributes = attributes;
        }

        @Override
        public YTreeNode onEndList() {
            List<YTreeNode> list = finishList();
            if (list.isEmpty() && attributes.isEmpty()) {
                return YTreeListNode.EMPTY;
            } else {
                return new YTreeListNode(list, attributes);
            }
        }
    }

    protected static class Result extends State {
        private final YTreeNode value;

        public Result(YTreeNode value) {
            this.value = value;
        }

        @Override
        public YTreeNode result() {
            return value;
        }
    }

    protected boolean isEmpty() {
        return stack.isEmpty();
    }

    protected void push(State state) {
        stack.add(state);
    }

    protected void push(YTreeNode value) {
        if (stack.isEmpty()) {
            push(new Result(value));
        } else {
            last().onValue(value);
        }
    }

    protected State last() {
        if (stack.isEmpty()) {
            throw new IllegalStateException("Not building a container");
        }
        return stack.get(stack.size() - 1);
    }

    protected <T> T pop(Function<State, T> fn) {
        T result = fn.apply(last());
        stack.remove(stack.size() - 1);
        return result;
    }

    protected Map<String, YTreeNode> takeAttributes() {
        Map<String, YTreeNode> attributes = currentAttributes;
        if (attributes != null) {
            currentAttributes = null;
        } else {
            attributes = Collections.emptyMap();
        }
        return attributes;
    }

    @Override
    public void onStringScalar(String value) {
        push(new YTreeStringNode(value, takeAttributes()));
    }

    @Override
    public void onStringScalar(byte[] value) {
        push(new YTreeStringNode(value, takeAttributes()));
    }

    @Override
    public void onInt64Scalar(long value) {
        push(new YTreeInt64Node(value, takeAttributes()));
    }

    @Override
    public void onUint64Scalar(long value) {
        push(new YTreeUint64Node(value, takeAttributes()));
    }

    @Override
    public void onDoubleScalar(double value) {
        push(new YTreeDoubleNode(value, takeAttributes()));
    }

    @Override
    public void onBooleanScalar(boolean value) {
        Map<String, YTreeNode> attributes = takeAttributes();
        if (attributes.isEmpty()) {
            push(value ? YTreeBooleanNode.TRUE : YTreeBooleanNode.FALSE);
        } else {
            push(new YTreeBooleanNode(value, attributes));
        }
    }

    @Override
    public void onEntity() {
        Map<String, YTreeNode> attributes = takeAttributes();
        if (attributes.isEmpty()) {
            push(YTreeEntityNode.INSTANCE);
        } else {
            push(new YTreeEntityNode(attributes));
        }
    }

    @Override
    public void onBeginList() {
        push(new ListBuilder(takeAttributes()));
    }

    @Override
    public void onListItem() {
        last().onListItem();
    }

    @Override
    public void onEndList() {
        push(pop(State::onEndList));
    }

    @Override
    public void onBeginMap() {
        push(new MapBuilder(takeAttributes()));
    }

    @Override
    public void onKeyedItem(String key) {
        last().onKeyedItem(key);
    }

    @Override
    public void onEndMap() {
        push(pop(State::onEndMap));
    }

    @Override
    public void onBeginAttributes() {
        if (currentAttributes != null) {
            throw new IllegalStateException("Cannot build multiple consecutive attribute blocks");
        }
        push(new AttributesBuilder());
    }

    @Override
    public void onEndAttributes() {
        if (currentAttributes != null) {
            throw new IllegalStateException("Cannot build multiple consecutive attribute blocks");
        }
        currentAttributes = pop(State::onEndAttributes);
    }

    public YTreeNode build() {
        if (stack.isEmpty()) {
            throw new IllegalStateException("Builder is empty");
        }
        if (currentAttributes != null) {
            throw new IllegalStateException("Unconsumed attributes found");
        }
        return pop(State::result);
    }

    public YTreeBuilder beginAttributes() {
        onBeginAttributes();
        return this;
    }

    public YTreeBuilder endAttributes() {
        onEndAttributes();
        return this;
    }

    public Map<String, YTreeNode> buildAttributes() {
        onEndAttributes();
        return takeAttributes();
    }

    public YTreeBuilder beginList() {
        onBeginList();
        return this;
    }

    public YTreeBuilder endList() {
        onEndList();
        return this;
    }

    public YTreeListNode buildList() {
        return (YTreeListNode) endList().build();
    }

    public YTreeBuilder beginMap() {
        onBeginMap();
        return this;
    }

    public YTreeBuilder endMap() {
        onEndMap();
        return this;
    }

    public YTreeMapNode buildMap() {
        return (YTreeMapNode) endMap().build();
    }

    public YTreeBuilder key(String key) {
        last().onKeyedItem(key);
        return this;
    }

    public YTreeBuilder value(Object value) {
        if (value == null) {
            onEntity();
        } else if (value instanceof YTreeConvertible) {
            last().onValue(((YTreeConvertible) value).toYTree());
        } else if (value instanceof String) {
            onStringScalar((String) value);
        } else if (value instanceof byte[]) {
            onStringScalar((byte[]) value);
        } else if (value instanceof Long) {
            onInt64Scalar((Long) value);
        } else if (value instanceof Integer) {
            onInt64Scalar((Integer) value);
        } else if (value instanceof Double) {
            onDoubleScalar((Double) value);
        } else if (value instanceof Float) {
            onDoubleScalar((Float) value);
        } else if (value instanceof Boolean) {
            onBooleanScalar((Boolean) value);
        } else if (value instanceof List) {
            beginList();
            for (Object child : ((List<?>) value)) {
                value(child);
            }
            endList();
        } else if (value instanceof Map) {
            beginMap();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                Object key = entry.getKey();
                if (!(key instanceof String)) {
                    throw new IllegalArgumentException("Only maps with string keys can be translated to YTreeNode");
                }
                key((String) key);
                value(entry.getValue());
            }
            endMap();
        } else {
            throw new IllegalArgumentException(
                    "Cannot translate " + value.getClass().getSimpleName() + " to YTreeNode");
        }
        return this;
    }

    public YTreeBuilder stringValue(String value) {
        onStringScalar(value);
        return this;
    }

    public YTreeBuilder stringValue(byte[] value) {
        onStringScalar(value);
        return this;
    }

    public YTreeBuilder signedValue(long value) {
        onInt64Scalar(value);
        return this;
    }

    public YTreeBuilder unsignedValue(long value) {
        onUint64Scalar(value);
        return this;
    }

    public YTreeBuilder doubleValue(double value) {
        onDoubleScalar(value);
        return this;
    }

    public YTreeBuilder booleanValue(boolean value) {
        onBooleanScalar(value);
        return this;
    }

    public YTreeBuilder entityValue() {
        onEntity();
        return this;
    }

    public YTreeBuilder nullValue() {
        onEntity();
        return this;
    }

    public YTreeBuilder listValue(List<?> value) {
        return value(value);
    }

    public YTreeBuilder mapValue(Map<String, ?> value) {
        return value(value);
    }

    public static YTreeBuilder newList() {
        return new YTreeBuilder().beginList();
    }

    public static YTreeBuilder newMap() {
        return new YTreeBuilder().beginMap();
    }
}
