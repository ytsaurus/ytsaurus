package tech.ytsaurus.client.operations;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

import javax.annotation.Nullable;

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
