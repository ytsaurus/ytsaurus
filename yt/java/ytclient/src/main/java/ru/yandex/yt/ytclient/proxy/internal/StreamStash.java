package ru.yandex.yt.ytclient.proxy.internal;

import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

import java.util.LinkedList;
import java.util.function.Consumer;

public abstract class StreamStash<DataType> {
    public abstract void push(DataType data, long offset);
    public abstract StashedMessage<DataType> read() throws Exception;
    public abstract void commit(RpcClientStreamControl control);

    public static <DataType> StreamStash<DataType> syncStash() {
        return new StreamStash<DataType>() {
            private final LinkedList<StashedMessage<DataType>> messages = new LinkedList<>();

            @Override
            public void push(DataType data, long offset) {
                synchronized (messages) {
                    messages.push(new StashedMessage<>(data, offset));
                    messages.notify();
                }
            }

            @Override
            public StashedMessage<DataType> read() throws Exception {
                synchronized (messages) {
                    while (messages.isEmpty()) {
                        messages.wait();
                    }

                    return messages.removeFirst();
                }
            }

            @Override
            public void commit(RpcClientStreamControl control) { }
        };
    }

    public static <DataType> StreamStash<DataType> asyncStash(Consumer<DataType> consumer) {
        return new StreamStash<DataType>() { // jdk8 compatibilty
            private long offset = 0;

            @Override
            public void push(DataType data, long offset) {
                this.offset = offset;
                consumer.accept(data);
            }

            @Override
            public StashedMessage<DataType> read() {
                throw new IllegalArgumentException();
            }

            @Override
            public void commit(RpcClientStreamControl control) {
                control.feedback(offset);
            }
        };
    }
}
