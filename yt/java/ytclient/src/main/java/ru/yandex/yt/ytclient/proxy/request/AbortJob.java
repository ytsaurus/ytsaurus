package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public class AbortJob extends ru.yandex.yt.ytclient.request.AbortJob.BuilderBase<
        AbortJob, ru.yandex.yt.ytclient.request.AbortJob> {
    public AbortJob(GUID jobId) {
        this(jobId, null);
    }

    public AbortJob(GUID jobId, @Nullable Long interruptTimeout) {
        setJobId(jobId);
        if (interruptTimeout != null) {
            setInterruptTimeout(interruptTimeout);
        }
    }

    @Override
    protected AbortJob self() {
        return this;
    }

    @Override
    public ru.yandex.yt.ytclient.request.AbortJob build() {
        return new ru.yandex.yt.ytclient.request.AbortJob(this);
    }
}
