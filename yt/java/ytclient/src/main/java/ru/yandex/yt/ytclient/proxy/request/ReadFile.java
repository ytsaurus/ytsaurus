package ru.yandex.yt.ytclient.proxy.request;

public class ReadFile extends ru.yandex.yt.ytclient.request.ReadFile.BuilderBase<ReadFile> {
    public ReadFile(String path) {
        setPath(path);
    }

    @Override
    protected ReadFile self() {
        return this;
    }
}
