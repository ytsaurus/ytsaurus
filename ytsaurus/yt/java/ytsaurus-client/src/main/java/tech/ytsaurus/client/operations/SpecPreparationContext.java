package tech.ytsaurus.client.operations;


import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Context required for preparation of specs.
 */
@NonNullApi
@NonNullFields
public class SpecPreparationContext {
    private final YTsaurusClientConfig configuration;

    public SpecPreparationContext(YTsaurusClientConfig configuration) {
        this.configuration = configuration;
    }

    public YTsaurusClientConfig getConfiguration() {
        return configuration;
    }
}
