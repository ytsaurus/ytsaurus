package tech.ytsaurus.client.rows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.IntFunction;

@FunctionalInterface
public interface ConsumerSource<T> extends Consumer<T> {

    default void setRowCount(int rowCount) {
        // do nothing
    }

    static <T> ConsumerSource<T> wrap(Consumer<T> consumer) {
        return consumer::accept;
    }


    static <T> ConsumerSourceRet<T> list() {
        return list(ArrayList::new);
    }


    static <T> ConsumerSourceRet<T> list(IntFunction<List<T>> function) {
        return new ConsumerSourceRet<T>() {

            private List<T> list;

            @Override
            public void setRowCount(int rowCount) {
                list = function.apply(rowCount);
            }

            @Override
            public void accept(T t) {
                if (list == null) {
                    list = new ArrayList<>();
                }
                list.add(t);
            }

            @Override
            public List<T> get() {
                return list != null ? list : Collections.emptyList();
            }
        };
    }

}
