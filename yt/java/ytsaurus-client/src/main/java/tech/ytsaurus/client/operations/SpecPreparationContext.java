package tech.ytsaurus.client.operations;


import tech.ytsaurus.client.YtClientConfiguration;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Context required for preparation of specs.
 */
@NonNullApi
@NonNullFields
public class SpecPreparationContext {
    private final YtClientConfiguration configuration;

    public SpecPreparationContext(YtClientConfiguration configuration) {
        this.configuration = configuration;
    }

    public YtClientConfiguration getConfiguration() {
        return configuration;
    }
}
