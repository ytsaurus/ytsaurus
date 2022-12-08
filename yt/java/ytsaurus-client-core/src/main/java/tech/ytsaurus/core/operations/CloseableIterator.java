package tech.ytsaurus.core.operations;

import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    default Stream<T> stream() {
        Iterable<T> iterable = () -> this;
        return StreamSupport.stream(iterable.spliterator(), false)
                .onClose(() -> {
                    try {
                        close();
                    } catch (Exception e) {
                        throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
                    }
                });
    }

    default <R> CloseableIterator<R> map(Function<? super T, ? extends R> mapper) {
        return new CloseableIterator<R>() {
            @Override
            public boolean hasNext() {
                return CloseableIterator.this.hasNext();
            }

            @Override
            public R next() {
                return mapper.apply(CloseableIterator.this.next());
            }

            @Override
            public void close() throws Exception {
                CloseableIterator.this.close();
            }
        };
    }

    static <T> CloseableIterator<T> wrap(Iterable<T> delegate) {
        return wrap(delegate.iterator());
    }

    static <T> CloseableIterator<T> wrap(Iterator<T> delegate) {
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public T next() {
                return delegate.next();
            }

            @Override
            public void close() throws Exception {
                if (delegate instanceof AutoCloseable) {
                    ((AutoCloseable) delegate).close();
                }
            }
        };
    }
}
