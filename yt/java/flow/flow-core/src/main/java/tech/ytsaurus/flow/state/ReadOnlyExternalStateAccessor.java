package tech.ytsaurus.flow.state;

import tech.ytsaurus.flow.row.Payload;

/**
 * Read-only {@link ExternalStateAccessor} over external state joined from another computation:
 * reads work as usual, {@link #set} and {@link #clear} throw {@link UnsupportedOperationException}
 * because a joiner is not a write-owner of the underlying state table.
 */
public class ReadOnlyExternalStateAccessor extends ExternalStateAccessor {

    /**
     * Intended to be called from {@link StateDescriptor#create}.
     */
    ReadOnlyExternalStateAccessor(
            Payload key,
            StatesHolder statesHolder
    ) {
        super(key, statesHolder);
    }

    /**
     * Always throws: joined external state is read-only.
     */
    @Override
    public void set(Payload value) {
        throw new UnsupportedOperationException(
                "Joined external state is read-only (StatesName: %s)".formatted(statesHolder.getName())
        );
    }

    /**
     * Always throws: joined external state is read-only.
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException(
                "Joined external state is read-only (StatesName: %s)".formatted(statesHolder.getName())
        );
    }
}
