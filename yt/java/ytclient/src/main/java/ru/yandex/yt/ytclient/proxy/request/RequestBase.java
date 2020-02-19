package ru.yandex.yt.ytclient.proxy.request;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.Nullable;

import com.google.protobuf.Message;

public class RequestBase<T extends RequestBase> {
    private Duration timeout;

    Message additionalData;

    Message getAdditionalData() {
        return additionalData;
    }

    T setAdditionalData(Message additionalData) {
        this.additionalData = additionalData;
        return (T) this;
    }

    public T setTimeout(@Nullable Duration timeout) {
        this.timeout = timeout;
        return (T) this;
    }

    public Optional<Duration> getTimeout() {
        return Optional.ofNullable(timeout);
    }
}
