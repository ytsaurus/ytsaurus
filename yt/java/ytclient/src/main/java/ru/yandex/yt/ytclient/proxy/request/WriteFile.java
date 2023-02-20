package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class WriteFile extends tech.ytsaurus.client.request.WriteFile.BuilderBase<WriteFile> {
    public WriteFile(String path) {
        setPath(path);
    }

    @Override
    protected WriteFile self() {
        return this;
    }
}
