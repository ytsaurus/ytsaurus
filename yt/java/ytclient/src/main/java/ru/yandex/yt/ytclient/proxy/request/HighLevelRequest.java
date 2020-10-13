package ru.yandex.yt.ytclient.proxy.request;

import com.google.protobuf.MessageLite;

import ru.yandex.yt.rpc.TRequestHeader;
import ru.yandex.yt.ytclient.rpc.RpcClientRequestBuilder;

public interface HighLevelRequest<T extends MessageLite.Builder> {
    String getArgumentsLogString();
    void writeHeaderTo(TRequestHeader.Builder header);
    void writeTo(RpcClientRequestBuilder<T, ?> builder);
}
