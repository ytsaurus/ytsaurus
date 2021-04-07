package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.yt.rpcproxy.TMasterReadOptions;

public class MasterReadOptions {
    private MasterReadKind readFrom = MasterReadKind.Follower;
    private Long expireAfterSuccessfulUpdateTime;
    private Long expireAfterFailedUpdateTime;
    private Integer cacheStickyGroupSize;

    public MasterReadOptions setReadFrom(MasterReadKind f) {
        readFrom = f;
        return this;
    }

    public MasterReadOptions setExpireAfterSuccessfulUpdateTime(long expireAfterSuccessfulUpdateTime) {
        this.expireAfterSuccessfulUpdateTime = expireAfterSuccessfulUpdateTime;
        return this;
    }

    public MasterReadOptions setExpireAfterFailedUpdateTime(long expireAfterFailedUpdateTime) {
        this.expireAfterFailedUpdateTime = expireAfterFailedUpdateTime;
        return this;
    }

    public MasterReadOptions setCacheStickyGroupSize(int cacheStickyGroupSize) {
        this.cacheStickyGroupSize = cacheStickyGroupSize;
        return this;
    }

    public TMasterReadOptions.Builder writeTo(TMasterReadOptions.Builder builder) {
        builder.setReadFrom(readFrom.getProtoValue());
        if (expireAfterSuccessfulUpdateTime != null) {
            builder.setExpireAfterSuccessfulUpdateTime(expireAfterSuccessfulUpdateTime);
        }
        if (expireAfterFailedUpdateTime != null) {
            builder.setExpireAfterFailedUpdateTime(expireAfterFailedUpdateTime);
        }
        if (cacheStickyGroupSize != null) {
            builder.setCacheStickyGroupSize(cacheStickyGroupSize);
        }
        return builder;
    }
}
