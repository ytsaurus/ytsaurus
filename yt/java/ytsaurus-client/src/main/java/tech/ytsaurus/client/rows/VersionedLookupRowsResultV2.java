package tech.ytsaurus.client.rows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.ApiServiceUtil;
import tech.ytsaurus.client.rpc.RpcClientResponse;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.rpcproxy.TRspVersionedLookupRows;

@NonNullApi
@NonNullFields
public class VersionedLookupRowsResultV2 extends LookupRowsResult<VersionedRowset> {
    private static final Logger logger = LoggerFactory.getLogger(VersionedLookupRowsResultV2.class);

    private final RpcClientResponse<TRspVersionedLookupRows> response;
    private final Executor heavyExecutor;

    VersionedLookupRowsResultV2(
            RpcClientResponse<TRspVersionedLookupRows> response,
            Executor heavyExecutor
    ) {
        super(response.body().getUnavailableKeyIndexesList());
        this.response = response;
        this.heavyExecutor = heavyExecutor;
    }

    public static Builder builder() { return new Builder(); }

    public static class Builder {
        private RpcClientResponse<TRspVersionedLookupRows> response;
        private Executor heavyExecutor;

        public Builder setResponse(RpcClientResponse<TRspVersionedLookupRows> response) {
            this.response = response;
            return this;
        }

        public Builder setHeavyExecutor(Executor heavyExecutor) {
            this.heavyExecutor = heavyExecutor;
            return this;
        }

        public VersionedLookupRowsResultV2 build() {
            if (response == null) throw new IllegalStateException("response must be set");
            return new VersionedLookupRowsResultV2(response, heavyExecutor);
        }
    }

    public CompletableFuture<VersionedRowset> getVersionedRowset() {
        return handleResponse(rsp -> ApiServiceUtil.deserializeVersionedRowset(
                rsp.body().getRowsetDescriptor(), rsp.attachments()));
    }

    private <T> CompletableFuture<T> handleResponse(Function<RpcClientResponse<TRspVersionedLookupRows>, T> fn) {
        return RpcUtil.applyAsync(
                CompletableFuture.completedFuture(response),
                rsp -> {
                    logger.trace("VersionedLookupRows incoming rowset descriptor: {}", rsp.body().getRowsetDescriptor());
                    return fn.apply(rsp);
                },
                heavyExecutor);
    }
}


