package tech.ytsaurus.ysontree;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.yson.YsonConsumer;

/**
 * Builder for {@link YTreeNode}
 *
 * Example of usage:
 * {@code
 *  YTreeNode mapNode = YTree.builder().buildMap()
 *      .key("foo").value(42)
 *      .key("bar").value("qux")
 *      .endMap().build();
 * }
 */
public class YTreeBuilder implements YsonConsumer {
    private final Deque<State> states = new ArrayDeque<>();

    public YTreeBuilder apply(Function<YTreeBuilder, YTreeBuilder> func) {
        return func.apply(this);
    }

    public YTreeBuilder value(Collection<?> c) {
        beginList();
        for (Object node : c) {
            value(node);
        }
        return endList();
    }

    public <T> YTreeBuilder value(Collection<? extends T> c, BiFunction<YTreeBuilder, ? super T, YTreeBuilder> f) {
        YTreeBuilder builder = beginList();
        for (T e : c) {
            builder = f.apply(builder, e);
        }
        return builder.endList();
    }

    public YTreeBuilder value(java.util.Map<String, ?> m) {
        beginMap();
        for (java.util.Map.Entry<String, ?> e : m.entrySet()) {
            key(e.getKey()).value(e.getValue());
        }
        return endMap();
    }

    public YTreeBuilder value(@Nullable Object obj) {
        if (obj == null) {
            return entity();
        } else if (obj instanceof YTreeNode) {
            return value((YTreeNode) obj);
        } else if (obj instanceof Boolean) {
            return value((boolean) obj);
        } else if (obj instanceof Float) {
            return value((float) obj);
        } else if (obj instanceof Double) {
            return value((double) obj);
        } else if (obj instanceof Integer) {
            return value((int) obj);
        } else if (obj instanceof Long) {
            return value((long) obj);
        } else if (obj.getClass().getName().equals("ru.yandex.misc.lang.number.UnsignedLong")) {
            try {
                Method method = obj.getClass().getMethod("longValue");
                return value((Long) method.invoke(obj), false);
            } catch (ReflectiveOperationException ex) {
                throw new RuntimeException(ex);
            }
        } else if (obj.getClass().getName().equals("ru.yandex.misc.lang.number.UnsignedInteger")) {
            try {
                Method method = obj.getClass().getMethod("longValue");
                return value((Long) method.invoke(obj), false);
            } catch (ReflectiveOperationException ex) {
                throw new RuntimeException(ex);
            }
        } else if (obj instanceof String) {
            return value((String) obj);
        } else if (obj instanceof byte[]) {
            return value((byte[]) obj);
        } else if (obj instanceof int[]) {
            return value((int[]) obj);
        } else if (obj instanceof long[]) {
            return value((long[]) obj);
        } else if (obj instanceof float[]) {
            return value((float[]) obj);
        } else if (obj instanceof double[]) {
            return value((double[]) obj);
        } else if (obj instanceof String[]) {
            return value((String[]) obj);
        } else if (obj instanceof Collection) {
            return value((Collection) obj);
        } else if (obj instanceof java.util.Map) {
            return value((java.util.Map) obj);
        } else {
            throw new IllegalArgumentException("Can't resolve method for " + obj.getClass().getName() + ": " + obj);
        }
    }

    public YTreeBuilder value(YTreeNode node) {
        YTreeNodeUtils.walk(node, this, true);
        return this;
    }

    public YTreeBuilder value(boolean value) {
        return push(attributes -> new YTreeBooleanNodeImpl(value, attributes));
    }

    public YTreeBuilder value(@Nullable Boolean value) {
        return value == null ? entity() : push(attributes -> new YTreeBooleanNodeImpl(value, attributes));
    }

    public YTreeBuilder value(double value) {
        return push(attributes -> new YTreeDoubleNodeImpl(value, attributes));
    }

    public YTreeBuilder value(@Nullable Double value) {
        return value == null ? entity() : push(attributes -> new YTreeDoubleNodeImpl(value, attributes));
    }

    public YTreeBuilder value(long value) {
        return push(attributes -> new YTreeIntegerNodeImpl(true, value, attributes));
    }

    public YTreeBuilder value(@Nullable Long value) {
        return value(value, true);
    }

    public YTreeBuilder value(@Nullable Long value, boolean signed) {
        return value == null ? entity() : push(attributes -> new YTreeIntegerNodeImpl(signed, value, attributes));
    }

    public YTreeBuilder unsignedValue(long value) {
        return push(attributes -> new YTreeIntegerNodeImpl(false, value, attributes));
    }

    public YTreeBuilder unsignedValue(@Nullable Long value) {
        return value == null ? entity() : push(attributes -> new YTreeIntegerNodeImpl(false, value, attributes));
    }

    public YTreeBuilder value(int value) {
        return push(attributes -> new YTreeIntegerNodeImpl(true, value, attributes));
    }

    public YTreeBuilder value(@Nullable Integer value) {
        return value == null ? entity() : push(attributes -> new YTreeIntegerNodeImpl(true, value, attributes));
    }

    public YTreeBuilder unsignedValue(int value) {
        return push(attributes -> new YTreeIntegerNodeImpl(false, value, attributes));
    }

    public YTreeBuilder unsignedValue(@Nullable Integer value) {
        return value == null ? entity() : push(attributes -> new YTreeIntegerNodeImpl(false, value, attributes));
    }

    public YTreeBuilder value(@Nullable String value) {
        return value == null ? entity() : push(attributes -> new YTreeStringNodeImpl(value, attributes));
    }

    public YTreeBuilder value(@Nullable byte[] values) {
        return values == null ? entity() : push(attributes -> new YTreeStringNodeImpl(values, attributes));
    }

    public YTreeBuilder value(int[] values) {
        beginList();
        for (int value : values) {
            value(value);
        }
        return endList();
    }

    public YTreeBuilder value(long[] values) {
        beginList();
        for (long value : values) {
            value(value);
        }
        return endList();
    }

    public YTreeBuilder value(float[] values) {
        beginList();
        for (float value : values) {
            value(value);
        }
        return endList();
    }

    public YTreeBuilder value(double[] values) {
        beginList();
        for (double value : values) {
            value(value);
        }
        return endList();
    }

    public YTreeBuilder value(String[] values) {
        beginList();
        for (String value : values) {
            value(value);
        }
        return endList();
    }

    public YTreeBuilder entity() {
        return push(YTreeEntityNodeImpl::new);
    }

    public YTreeBuilder when(boolean condition, Function<YTreeBuilder, YTreeBuilder> callback) {
        if (condition) {
            return callback.apply(this);
        } else {
            return this;
        }
    }

    public <T> YTreeBuilder forEach(Iterable<T> iterable, BiConsumer<YTreeBuilder, T> actionPerElement) {
        return forEach(iterable.iterator(), actionPerElement);
    }

    public <T> YTreeBuilder forEach(Iterator<T> iterator, BiConsumer<YTreeBuilder, T> actionPerElement) {
        while (iterator.hasNext()) {
            actionPerElement.accept(this, iterator.next());
        }
        return this;
    }

    public YTreeBuilder beginList() {
        Map<String, YTreeNode> attributes = getAttributes();
        YTreeListNodeImpl node = new YTreeListNodeImpl(attributes);
        states.push(new ListState(node));
        return this;
    }

    public YTreeBuilder endList() {
        if (states.isEmpty() || !(states.peek() instanceof ListState)) {
            throw new IllegalStateException();
        }
        ListState list = (ListState) states.pop();
        return push(list.node);
    }

    public YTreeBuilder beginMap() {
        Map<String, YTreeNode> attributes = getAttributes();
        YTreeMapNodeImpl node = new YTreeMapNodeImpl(attributes);
        states.push(new MapState(node));
        return this;
    }

    public YTreeBuilder endMap() {
        if (states.isEmpty() || !(states.peek() instanceof MapState)) {
            throw new IllegalStateException();
        }
        MapState map = (MapState) states.pop();
        return push(map.node);
    }

    public YTreeBuilder beginAttributes() {
        if (!states.isEmpty() && states.peek() instanceof AttributesState) {
            throw new IllegalArgumentException();
        }
        states.push(new AttributesState());
        return this;
    }

    public YTreeBuilder endAttributes() {
        if (states.isEmpty() || !(states.peek() instanceof AttributesState)) {
            throw new IllegalStateException();
        }
        AttributesState attr = (AttributesState) states.peek();
        if (attr.finished) {
            throw new IllegalStateException();
        }
        attr.finished = true;
        return this;
    }

    public YTreeBuilder key(String key) {
        states.push(new KeyedItemState(key));
        return this;
    }

    public YTreeNode build() {
        if (states.size() != 1) {
            throw new IllegalStateException();
        }
        State s = states.pop();
        if (!(s instanceof ResultState)) {
            throw new IllegalStateException();
        }
        return ((ResultState) s).node;
    }

    public YTreeMapNode buildMap() {
        return endMap().build().mapNode();
    }

    public YTreeListNode buildList() {
        return endList().build().listNode();
    }

    public Map<String, YTreeNode> buildAttributes() {
        Map<String, YTreeNode> attributes = endAttributes().getAttributes();
        if (!states.isEmpty()) {
            throw new IllegalStateException();
        }
        return attributes;
    }

    /* methods for consumer */

    @Override
    public void onInteger(long value) {
        value(value);
    }

    @Override
    public void onUnsignedInteger(long value) {
        value(value, false);
    }

    @Override
    public void onBoolean(boolean value) {
        value(value);
    }

    @Override
    public void onDouble(double value) {
        value(value);
    }

    @Override
    public void onString(@Nonnull String value) {
        value(value);
    }

    @Override
    public void onString(@Nonnull byte[] bytes, int offset, int length) {
        value(Arrays.copyOfRange(bytes, offset, offset + length));
    }

    @Override
    public void onEntity() {
        entity();
    }

    @Override
    public void onListItem() {

    }

    @Override
    public void onBeginList() {
        beginList();
    }

    @Override
    public void onEndList() {
        endList();
    }

    @Override
    public void onBeginAttributes() {
        beginAttributes();
    }

    @Override
    public void onEndAttributes() {
        endAttributes();
    }

    @Override
    public void onBeginMap() {
        beginMap();
    }

    @Override
    public void onEndMap() {
        endMap();
    }

    @Override
    public void onKeyedItem(@Nonnull byte[] value, int offset, int length) {
        // NB. this is compatible with old behaviour
        onKeyedItem(new String(value, offset, length, StandardCharsets.UTF_8));
    }

    @Override
    public void onKeyedItem(@Nonnull String key) {
        key(key);
    }

    /* private classes and methods */

    private static class State {
    }

    private static class ResultState extends State {
        private final YTreeNode node;

        ResultState(YTreeNode node) {
            this.node = node;
        }

    }

    private static class ListState extends State {
        private final YTreeListNode node;

        ListState(YTreeListNode node) {
            this.node = node;
        }
    }

    private static class MapState extends State {
        private final YTreeMapNode node;

        MapState(YTreeMapNode node) {
            this.node = node;
        }
    }

    private static class AttributesState extends State {
        private final Map<String, YTreeNode> map = new HashMap<>();
        private boolean finished = false;
    }

    private static class KeyedItemState extends State {
        private final String key;

        KeyedItemState(String key) {
            this.key = key;
        }
    }

    private Map<String, YTreeNode> getAttributes() {
        Map<String, YTreeNode> attributes = null;
        if (!states.isEmpty() && states.peek() instanceof AttributesState) {
            AttributesState attr = (AttributesState) states.peek();
            if (!attr.finished) {
                throw new IllegalStateException();
            }
            attributes = attr.map;
            states.pop();
        }
        return attributes;
    }

    private YTreeBuilder push(YTreeNode node) {
        if (states.isEmpty()) {
            states.push(new ResultState(node));
            return this;
        }
        if (states.peek() instanceof KeyedItemState) {
            String key = ((KeyedItemState) states.pop()).key;
            if (states.isEmpty()) {
                throw new IllegalStateException();
            } else if (states.peek() instanceof MapState) {
                MapState map = (MapState) states.peek();
                map.node.put(key, node);
                return this;
            } else if (states.peek() instanceof AttributesState) {
                AttributesState attr = (AttributesState) states.peek();
                if (attr.finished) {
                    throw new IllegalStateException();
                }
                attr.map.put(key, node);
                return this;
            } else {
                throw new IllegalStateException();
            }
        }
        if (states.peek() instanceof ListState) {
            ListState list = (ListState) states.peek();
            list.node.add(node);
            return this;
        }
        throw new IllegalStateException();
    }

    private YTreeBuilder push(Function<Map<String, YTreeNode>, YTreeNode> function) {
        Map<String, YTreeNode> attributes = getAttributes();
        return push(function.apply(attributes));
    }
}
