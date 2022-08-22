package ru.yandex.yt.ytclient;

import javax.annotation.Nullable;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.rpc.RpcOptions;

@NonNullApi
@NonNullFields
public class YtClientConfiguration {
    private final RpcOptions rpcOptions;

    YtClientConfiguration(Builder builder) {
        this.rpcOptions = builder.rpcOptions;
    }

    public static Builder builder() {
        return new Builder();
    }

    public RpcOptions getRpcOptions() {
        return rpcOptions;
    }

    @NonNullApi
    @NonNullFields
    public static class Builder {
        @Nullable
        private RpcOptions rpcOptions;

        public Builder setRpcOptions(RpcOptions rpcOptions) {
            this.rpcOptions = rpcOptions;
            return this;
        }

        public YtClientConfiguration build() {
            if (rpcOptions == null) {
                rpcOptions = new RpcOptions();
            }
            return new YtClientConfiguration(this);
        }
    }
}
