package ru.yandex.yt.ytclient.operations;

import ru.yandex.yt.ytclient.YtClientConfiguration;

public class SpecPreparationContext {
    private final YtClientConfiguration configuration;

    public SpecPreparationContext(YtClientConfiguration configuration) {
        this.configuration = configuration;
    }

    public YtClientConfiguration getConfiguration() {
        return configuration;
    }
}
