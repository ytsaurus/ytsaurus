package tech.ytsaurus.client.operations;


import tech.ytsaurus.client.YtClientConfiguration;

public class SpecPreparationContext {
    private final YtClientConfiguration configuration;

    public SpecPreparationContext(YtClientConfiguration configuration) {
        this.configuration = configuration;
    }

    public YtClientConfiguration getConfiguration() {
        return configuration;
    }
}
