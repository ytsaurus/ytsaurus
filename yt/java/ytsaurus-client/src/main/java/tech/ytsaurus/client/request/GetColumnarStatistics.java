package tech.ytsaurus.client.request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.ByteString;
import tech.ytsaurus.client.rpc.RpcClientRequestBuilder;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.rpcproxy.TReqGetColumnarStatistics;
import tech.ytsaurus.rpcproxy.TTransactionalOptions;

/**
 * Every path must specify columns via {@link YPath#withColumns}; statistics follow their order.
 * Returns one {@link ColumnarStatistics} per path. Uses the server's default fetcher settings.
 */
public class GetColumnarStatistics
        extends RequestBase<GetColumnarStatistics.Builder, GetColumnarStatistics>
        implements HighLevelRequest<TReqGetColumnarStatistics.Builder> {
    private final List<YPath> paths;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    GetColumnarStatistics(Builder builder) {
        super(builder);
        this.paths = new ArrayList<>(Objects.requireNonNull(builder.paths));
        if (builder.transactionalOptions != null) {
            this.transactionalOptions = new TransactionalOptions(builder.transactionalOptions);
        } else {
            this.transactionalOptions = null;
        }
    }

    public GetColumnarStatistics(List<YPath> paths) {
        this(builder().setPaths(paths));
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(RpcClientRequestBuilder<TReqGetColumnarStatistics.Builder, ?> requestBuilder) {
        TReqGetColumnarStatistics.Builder builder = requestBuilder.body();
        for (YPath path : paths) {
            builder.addPaths(ByteString.copyFrom(path.toTree().toBinary()));
        }
        if (transactionalOptions != null) {
            builder.setTransactionalOptions(transactionalOptions.writeTo(TTransactionalOptions.newBuilder()));
        }
        if (additionalData != null) {
            builder.mergeFrom(additionalData);
        }
    }

    @Override
    protected void writeArgumentsLogString(@Nonnull StringBuilder sb) {
        super.writeArgumentsLogString(sb);
        sb.append("Paths: ").append(paths).append(";");
    }

    @Override
    public Builder toBuilder() {
        return builder()
                .setPaths(paths)
                .setTransactionalOptions(transactionalOptions)
                .setTimeout(timeout)
                .setRequestId(requestId)
                .setUserAgent(userAgent)
                .setTraceId(traceId, traceSampled)
                .setAdditionalData(additionalData);
    }

    public static class Builder extends RequestBase.Builder<Builder, GetColumnarStatistics> {
        @Nullable
        private List<YPath> paths;
        @Nullable
        private TransactionalOptions transactionalOptions;

        public Builder setPaths(List<YPath> paths) {
            this.paths = paths;
            return self();
        }

        public Builder setPaths(YPath... paths) {
            setPaths(Arrays.asList(paths));
            return self();
        }

        public Builder setTransactionalOptions(@Nullable TransactionalOptions transactionalOptions) {
            this.transactionalOptions = transactionalOptions;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        public GetColumnarStatistics build() {
            return new GetColumnarStatistics(this);
        }
    }
}
