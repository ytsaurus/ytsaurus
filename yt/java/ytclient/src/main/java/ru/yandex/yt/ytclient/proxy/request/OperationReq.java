package ru.yandex.yt.ytclient.proxy.request;

import java.util.function.Consumer;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.TGuid;
import ru.yandex.yt.ytclient.rpc.RpcUtil;

@NonNullApi
@NonNullFields
abstract class OperationReq<T extends OperationReq<T>> extends RequestBase<T> {
    final @Nullable GUID operationId;
    final @Nullable String operationAlias;

    OperationReq(@Nullable GUID operationId, @Nullable String operationAlias) {
        if (operationId == null && operationAlias == null) {
            throw new NullPointerException();
        }
        this.operationId = operationId;
        this.operationAlias = operationAlias;
    }

    void writeOperationDescriptionToProto(Consumer<TGuid> operationIdSetter, Consumer<String> operationAliasSetter) {
        if (operationId != null) {
            operationIdSetter.accept(RpcUtil.toProto(operationId));
        } else {
            operationAliasSetter.accept(operationAlias);
        }
    }

    YTreeBuilder toTree(YTreeBuilder builder) {
        if (operationId != null) {
            builder.key("operation_id").value(operationId.toString());
        } else {
            builder.key("operation_alias").value(operationAlias);
        }
        return builder;
    }

    @Override
    protected void writeArgumentsLogString(StringBuilder sb) {
        if (operationId != null) {
            sb.append("Id: ").append(operationId).append("; ");
        } else {
            sb.append("Alias: ").append(operationAlias).append("; ");
        }
    }
}
