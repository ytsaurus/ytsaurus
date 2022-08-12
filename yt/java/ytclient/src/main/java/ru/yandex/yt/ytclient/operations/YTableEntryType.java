package ru.yandex.yt.ytclient.operations;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.function.Consumer;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer.YTreeRetriableIterator.RetryState;
import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.tables.CloseableIterator;
import ru.yandex.inside.yt.kosher.tables.RetriableIterator;
import ru.yandex.inside.yt.kosher.tables.TableReaderPipe;
import ru.yandex.inside.yt.kosher.ytree.YTreeStringNode;

/**
 * @author sankear
 */
public interface YTableEntryType<T> extends Serializable {

    YTreeStringNode format();

    // either one should be implemented
    default CloseableIterator<T> iterator(InputStream input) {
        return iterator(input, new OperationContext());
    }

    default CloseableIterator<T> iterator(InputStream input, OperationContext context) {
        return iterator(input);
    }

    // either one should be implemented
    default RetriableIterator<T> iterator(YPath path, InputStream input, long startRowIndex) {
        RetryState state = new RetryState();
        state.startRowIndex = startRowIndex;
        return iterator(path, input, state);
    }

    default RetriableIterator<T> iterator(YPath path, InputStream input, RetryState state) {
        return iterator(path, input, state.startRowIndex);
    }

    default TableReaderPipe pipe(Consumer<T> consumer) {
        throw new RuntimeException();
    }

    Yield<T> yield(OutputStream[] output);

}
