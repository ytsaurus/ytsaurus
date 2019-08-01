package ru.yandex.yt.ytclient.proxy.internal;

import java.util.LinkedList;
import java.util.function.Function;

import ru.yandex.yt.ytclient.rpc.RpcClientStreamControl;

public abstract class StreamStash<DataType> {
    public abstract void push(DataType data, long offset);
    public abstract StashedMessage<DataType> read() throws Exception;
    public abstract void commit(RpcClientStreamControl control);
    public abstract LinkedList<StashedMessage<DataType>> messages();

    static <DataType> StreamStash<DataType> syncStash(LinkedList<StashedMessage<DataType>> oldMessages) {
        return new StreamStash<DataType>() { // jdk8 compatibilty
            private final LinkedList<StashedMessage<DataType>> messages = oldMessages;

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
            public LinkedList<StashedMessage<DataType>> messages() {
                return this.messages;
            }

            @Override
            public void commit(RpcClientStreamControl control) { }
        };
    }

    static <DataType> StreamStash<DataType> asyncStash(Function<DataType, Boolean> function, LinkedList<StashedMessage<DataType>> oldMessages) {
        return new StreamStash<DataType>() { // jdk8 compatibilty
            private final LinkedList<StashedMessage<DataType>> messages = oldMessages;
            private long offset = 0;

            @Override
            public void push(DataType data, long offset) {
                if (function.apply(data)) {
                    this.offset = offset;
                } else {
                    messages.push(new StashedMessage<>(data, offset));
                }
            }

            @Override
            public LinkedList<StashedMessage<DataType>> messages() {
                return this.messages;
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
