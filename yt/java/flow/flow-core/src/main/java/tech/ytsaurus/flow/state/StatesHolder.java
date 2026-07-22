package tech.ytsaurus.flow.state;

import java.util.HashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.client.rows.UnversionedRow;
import tech.ytsaurus.core.tables.TableSchema;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.PayloadBuilder;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeConvertible;
import tech.ytsaurus.ysontree.YTreeNode;

/**
 * Holder of raw binary state representation for particular state.
 * This class doesn't put any restriction to the way of state serialization.
 *
 * <p>This class is <b>not thread-safe</b>. An instance is created per incoming request and is
 * confined to that single request-processing thread for its entire lifetime; it must not be
 * shared across threads.
 */
public class StatesHolder<T extends State<?>> implements YTreeConvertible {
    private final String name;
    private final @Nullable TableSchema keySchema;
    private final @Nullable TableSchema stateSchema;
    private final Map<UnversionedRow, T> states;
    /**
     * States whose value was changed during the current epoch via {@link #set} by state accessors.
     */
    private final Map<UnversionedRow, T> modifiedStates;
    /**
     * Memoized empty payload returned by {@link #emptyStatePayload}.
     */
    private @Nullable Payload emptyStatePayload;

    public StatesHolder(
            String name,
            @Nullable TableSchema keySchema
    ) {
        this(name, keySchema, null);
    }

    public StatesHolder(
            String name,
            @Nullable TableSchema keySchema,
            @Nullable TableSchema stateSchema
    ) {
        this.name = name;
        this.keySchema = keySchema;
        this.stateSchema = stateSchema;
        this.states = new HashMap<>();
        this.modifiedStates = new HashMap<>();
    }

    /**
     * Set binary value for key. Marks the key as modified so it is included in the response sent
     * back to the companion computation. Used for writes coming from state accessors.
     *
     * @param key   UnversionedRow key.
     * @param value State value.
     */
    public void set(UnversionedRow key, T value) {
        this.states.put(key, value);
        this.modifiedStates.put(key, value);
    }

    /**
     * Load a value for key WITHOUT marking it as modified. Used to populate the holder from the
     * incoming request: such states must not be echoed back unless an accessor changes them.
     *
     * @param key   UnversionedRow key.
     * @param value State value.
     */
    public void load(UnversionedRow key, T value) {
        this.states.put(key, value);
    }

    /**
     * Get value for key.
     *
     * @param key UnversionedRow key.
     * @return State value, or {@code null} if absent.
     */
    public @Nullable T get(UnversionedRow key) {
        return states.get(key);
    }

    /**
     * Get name of state.
     *
     * @return Name of state.
     */
    public String getName() {
        return name;
    }

    /**
     * Get all states.
     *
     * @return Map of states.
     */
    public Map<UnversionedRow, T> getStates() {
        return states;
    }

    /**
     * States modified during the current epoch via {@link #set} by state accessors, keyed by
     * their {@link UnversionedRow} key.
     *
     * <p>Returned for allocation-free iteration on the response-encoding hot path.
     * Callers must not mutate it.
     *
     * @return Map of modified states.
     */
    public Map<UnversionedRow, T> getModifiedStates() {
        return modifiedStates;
    }

    /**
     * Whether any state was modified during the current epoch via {@link #set}.
     *
     * @return {@code true} if there is at least one modified state.
     */
    public boolean hasModifiedStates() {
        return !modifiedStates.isEmpty();
    }

    /**
     * Get schema of state. Schema might be null for internal states.
     *
     * @return Schema of state.
     */
    public @Nullable TableSchema getStateSchema() {
        return stateSchema;
    }

    /**
     * Returns the memoized empty payload built from this holder's state schema, shared as the
     * default value for payload states. The instance is immutable and shared to avoid per-call
     * allocation.
     *
     * @return shared empty payload.
     * @throws UnsupportedOperationException if this holder has no schema.
     */
    Payload emptyStatePayload() {
        if (stateSchema == null) {
            throw new UnsupportedOperationException(
                    "Default value construction is not supported for state '" + name + "' (no schema)");
        }
        if (emptyStatePayload == null) {
            emptyStatePayload = new PayloadBuilder(stateSchema).finish();
        }
        return emptyStatePayload;
    }

    @Override
    public String toString() {
        return toYTree().toString();
    }

    @Override
    public YTreeNode toYTree() {
        YTreeBuilder builder = YTree.builder().beginMap()
                .key("state_name").value(name);
        if (stateSchema != null) {
            builder.key("state_schema").value(stateSchema.toYTree());
        }
        builder.key("state_items")
                .beginList();
        for (var stateItem : states.entrySet()) {
            builder.beginMap()
                    .key("key").value(stateItem.getKey().toYTreeMap(keySchema));
            if (stateItem.getValue() != null) {
                builder.key("state_payload").value(stateItem.getValue().toYTree());
            }
            builder.endMap();
        }
        builder.endList();
        return builder.endMap().build();
    }
}
