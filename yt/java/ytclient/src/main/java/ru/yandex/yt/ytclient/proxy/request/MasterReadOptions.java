package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TMasterReadOptions;

public class MasterReadOptions {
    private TMasterReadOptions.EMasterReadKind readFrom = TMasterReadOptions.EMasterReadKind.FOLLOWER;
    private Long successExpirationTime;
    private Long failureExpirationTime;
    private Integer cacheStickyGroupSize;

    public MasterReadOptions setReadFrom(TMasterReadOptions.EMasterReadKind f) {
        readFrom = f;
        return this;
    }

    public MasterReadOptions setSuccessExpirationTime(long successExpirationTime) {
        this.successExpirationTime = successExpirationTime;
        return this;
    }

    public MasterReadOptions setFailureExpirationTime(long failureExpirationTime) {
        this.failureExpirationTime = failureExpirationTime;
        return this;
    }

    public MasterReadOptions setCacheStickyGroupSize(int cacheStickyGroupSize) {
        this.cacheStickyGroupSize = cacheStickyGroupSize;
        return this;
    }

    public TMasterReadOptions.Builder writeTo(TMasterReadOptions.Builder builder) {
        builder.setReadFrom(readFrom);
        if (successExpirationTime != null) {
            builder.setSuccessExpirationTime(successExpirationTime);
        }
        if (failureExpirationTime != null) {
            builder.setFailureExpirationTime(failureExpirationTime);
        }
        if (cacheStickyGroupSize != null) {
            builder.setCacheStickyGroupSize(cacheStickyGroupSize);
        }
        return builder;
    }
}
