package tech.ytsaurus.client.operations;

import java.util.Iterator;
import java.util.function.Function;

import javax.annotation.Nullable;

public class ReducerWithKeyIterator<TInput, TKey> implements Iterator<TInput> {

    private boolean eof = false;
    private boolean hasNextChecked = false;
    private TInput current;
    private TKey key;
    private TKey groupKey;

    private final Function<TInput, TKey> keyF;
    private final Iterator<TInput> iterator;

    public ReducerWithKeyIterator(Function<TInput, TKey> keyF, Iterator<TInput> iterator) {
        this.iterator = iterator;
        this.keyF = keyF;
        if (!iterator.hasNext()) {
            eof = true;
            current = null;
            key = null;
        } else {
            current = iterator.next();
            key = keyF.apply(current);
        }
    }

    @Nullable
    public TKey nextKey() {
        if (eof) {
            return null;
        }
        groupKey = key;
        return groupKey;
    }

    @Override
    public boolean hasNext() {
        hasNextChecked = true;
        if (eof) {
            return false;
        }
        return groupKey.equals(key);
    }

    @Override
    public TInput next() {
        if (!hasNextChecked && !hasNext()) {
            throw new IllegalStateException();
        }
        hasNextChecked = false;
        TInput ret = current;
        if (!iterator.hasNext()) {
            eof = true;
        } else {
            current = iterator.next();
            key = keyF.apply(current);
        }
        return ret;
    }

}
