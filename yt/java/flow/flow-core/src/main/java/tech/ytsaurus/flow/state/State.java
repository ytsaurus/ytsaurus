package tech.ytsaurus.flow.state;

import java.util.Arrays;
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

public class State<Value> implements YTreeConvertible {
    private final boolean reset;
    private final @Nullable Value value;

    public State(Value value) {
        this.value = value;
        this.reset = false;
    }

    public State(boolean reset, @Nullable Value value) {
        this.reset = reset;
        this.value = value;
    }

    public @Nullable Value getValue() {
        return value;
    }

    public boolean isReset() {
        return reset;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        State<?> state = (State<?>) o;
        return reset == state.reset && valueEquals(value, state.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(reset, valueHashCode(value));
    }

    private static boolean valueEquals(Object a, Object b) {
        if (a instanceof byte[] && b instanceof byte[]) {
            return Arrays.equals((byte[]) a, (byte[]) b);
        }
        return Objects.equals(a, b);
    }

    private static int valueHashCode(Object value) {
        if (value instanceof byte[]) {
            return Arrays.hashCode((byte[]) value);
        }
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        var builder = YTree.builder().beginMap()
                .key("reset")
                .value(reset);
        if (value != null && YTreeConvertible.class.isAssignableFrom(value.getClass())) {
            builder.key("state").value(((YTreeConvertible) value).toYTree());
        }
        return builder.endMap().build();
    }
}
