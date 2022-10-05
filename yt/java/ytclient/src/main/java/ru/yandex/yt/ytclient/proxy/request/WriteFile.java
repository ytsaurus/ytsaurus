package ru.yandex.yt.ytclient.proxy.request;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class WriteFile extends ru.yandex.yt.ytclient.request.WriteFile.BuilderBase<WriteFile> {
    public WriteFile(String path) {
        setPath(path);
    }

    @Override
    protected WriteFile self() {
        return this;
    }
}
