package ru.yandex.yt.ytclient.proxy.request;

public class ReadFile extends tech.ytsaurus.client.request.ReadFile.BuilderBase<ReadFile> {
    public ReadFile(String path) {
        setPath(path);
    }

    @Override
    protected ReadFile self() {
        return this;
    }
}
