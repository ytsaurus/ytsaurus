package ru.yandex.yt.ytclient.operations;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.tables.CloseableIterator;
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

    Yield<T> yield(OutputStream[] output);

}
