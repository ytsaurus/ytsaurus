package ru.yandex.yt.ytclient.request;

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
abstract class OperationReq<
        TBuilder extends OperationReq.Builder<TBuilder, TRequest>,
        TRequest extends OperationReq<TBuilder, TRequest>>
        extends RequestBase<TBuilder, TRequest> {
    @Nullable
    protected final GUID operationId;
    @Nullable
    protected final String operationAlias;

    OperationReq(Builder<?, ?> builder) {
        super(builder);
        if (builder.operationId == null && builder.operationAlias == null) {
            throw new NullPointerException();
        }
        this.operationId = builder.operationId;
        this.operationAlias = builder.operationAlias;
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

    public abstract static class Builder<
            TBuilder extends Builder<TBuilder, TRequest>,
            TRequest extends RequestBase<?, TRequest>>
            extends RequestBase.Builder<TBuilder, TRequest> {
        @Nullable
        protected GUID operationId;
        @Nullable
        protected String operationAlias;

        Builder() {
        }

        Builder(Builder<?, ?> builder) {
            super(builder);
            this.operationId = builder.operationId;
            this.operationAlias = builder.operationAlias;
        }

        public TBuilder setOperationId(@Nullable GUID operationId) {
            this.operationId = operationId;
            return self();
        }

        public TBuilder setOperationAlias(@Nullable String operationAlias) {
            this.operationAlias = operationAlias;
            return self();
        }

        void writeOperationDescriptionToProto(
                Consumer<TGuid> operationIdSetter, Consumer<String> operationAliasSetter) {
            if (operationId != null) {
                operationIdSetter.accept(RpcUtil.toProto(operationId));
            } else {
                operationAliasSetter.accept(operationAlias);
            }
        }

        protected YTreeBuilder toTree(YTreeBuilder builder) {
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
}
