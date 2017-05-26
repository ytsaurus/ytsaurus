package ru.yandex.yt.ytclient.rpc;

import java.io.IOException;

import com.google.protobuf.CodedInputStream;

@FunctionalInterface
public interface RpcMessageParser<T> {
    T parse(CodedInputStream input) throws IOException;
}
