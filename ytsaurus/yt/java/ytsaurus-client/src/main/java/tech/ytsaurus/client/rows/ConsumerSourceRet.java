package tech.ytsaurus.client.rows;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

@FunctionalInterface
public interface ConsumerSourceRet<T> extends ConsumerSource<T>, Supplier<List<T>> {

    @Override
    default List<T> get() {
        return Collections.emptyList(); // return empty list
    }

    static <T> ConsumerSourceRet<T> wrap(ConsumerSourceRet<T> consumer, Consumer<T> action) {
        return new ConsumerSourceRet<T>() {

            @Override
            public void setRowCount(int rowCount) {
                consumer.setRowCount(rowCount);
            }

            @Override
            public void accept(T t) {
                action.accept(t);
                consumer.accept(t);
            }

            @Override
            public List<T> get() {
                return consumer.get();
            }
        };
    }
}
