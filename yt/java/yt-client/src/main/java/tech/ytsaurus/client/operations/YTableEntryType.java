package tech.ytsaurus.client.operations;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import tech.ytsaurus.ysontree.YTreeStringNode;

import ru.yandex.inside.yt.kosher.operations.OperationContext;
import ru.yandex.inside.yt.kosher.operations.Yield;
import ru.yandex.inside.yt.kosher.tables.CloseableIterator;

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
