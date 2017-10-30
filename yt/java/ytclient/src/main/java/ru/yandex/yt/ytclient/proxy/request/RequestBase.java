package ru.yandex.yt.ytclient.proxy.request;

import com.google.protobuf.Message;

public class RequestBase<T extends RequestBase> {
    Message additionalData;

    Message getAdditionalData() {
        return additionalData;
    }

    T setAdditionalData(Message additionalData) {
        this.additionalData = additionalData;
        return (T)this;
    }
}
