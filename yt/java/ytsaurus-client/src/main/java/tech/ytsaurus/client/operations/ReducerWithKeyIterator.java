package tech.ytsaurus.client.operations;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nullable;

/**
 * Iterator that groups consecutive items by key.
 * Call {@link #nextKey()} to select the next group key; then {@link #hasNext()}, {@link #next()} and
 * the internal {@code getNext()} advance within the current group while item keys equal the selected
 * group key. When the underlying key changes, {@link #hasNext()} becomes false until {@link #nextKey()}
 * is called again.
 * See also: {@code tech.ytsaurus.client.operations.ReducerWithKey} and
 * {@code tech.ytsaurus.client.operations.ReduceOperationTest}.
 */
public class ReducerWithKeyIterator<TInput, TKey> implements Iterator<TInput> {

    private final Function<TInput, TKey> keyF;
    private final Iterator<TInput> iterator;

    private boolean eof = false;

    @Nullable
    private TInput current;
    @Nullable
    private TKey currentKey;
    @Nullable
    private TKey groupKey;

    public ReducerWithKeyIterator(Function<TInput, TKey> keyF, Iterator<TInput> iterator) {
        this.iterator = iterator;
        this.keyF = keyF;
    }

    /**
     * Selects and returns the next group key or {@code null} if there are no more items.
     * Subsequent {@link #hasNext()}/{@link #next()} calls will iterate items with this key only.
     */
    @Nullable
    public TKey nextKey() {
        if (eof) {
            return null;
        }
        if (currentKey == null) {
            getNext();
            if (eof) {
                return null;
            }
        }
        groupKey = currentKey;
        return groupKey;
    }

    /**
     * Returns {@code true} while there are items in the current group selected by {@link #nextKey()}.
     */
    @Override
    public boolean hasNext() {
        if (eof || !Objects.equals(groupKey, currentKey)) {
            return false;
        }
        if (current == null) {
            getNext();
        }
        return !eof && Objects.equals(groupKey, currentKey);
    }

    /**
     * Returns the next item from the current group.
     * Throws {@link IllegalStateException} if the group is exhausted or {@link #nextKey()} was not called.
     */
    @Override
    public TInput next() {
        if (!hasNext()) {
            throw new IllegalStateException();
        }
        TInput ret = current;
        current = null;
        return Objects.requireNonNull(ret);
    }

    private void getNext() {
        if (!iterator.hasNext()) {
            eof = true;
            return;
        }
        current = iterator.next();
        currentKey = keyF.apply(current);
    }
}
