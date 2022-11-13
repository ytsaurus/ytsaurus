package ru.yandex.yt.ytclient.request;

import com.google.protobuf.MessageLite;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;

import ru.yandex.yt.rpc.TRequestHeader;

public interface HighLevelRequest<T extends MessageLite.Builder> {
    String getArgumentsLogString();
    void writeHeaderTo(TRequestHeader.Builder header);
    void writeTo(RpcClientRequestBuilder<T, ?> builder);
}
