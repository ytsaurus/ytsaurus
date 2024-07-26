package tech.ytsaurus.client.operations;


import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.YTsaurusClientConfig;
import tech.ytsaurus.client.request.TransactionalOptions;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Context required for preparation of specs.
 */
@NonNullApi
@NonNullFields
public class SpecPreparationContext {
    private final YTsaurusClientConfig configuration;
    @Nullable
    private final TransactionalOptions transactionalOptions;

    public SpecPreparationContext(YTsaurusClientConfig configuration) {
        this.configuration = configuration;
        this.transactionalOptions = null;
    }

    public SpecPreparationContext(
            YTsaurusClientConfig configuration,
            @Nullable TransactionalOptions transactionalOptions
    ) {
        this.configuration = configuration;
        this.transactionalOptions = transactionalOptions;
    }

    public YTsaurusClientConfig getConfiguration() {
        return configuration;
    }

    public Optional<TransactionalOptions> getTransactionalOptions() {
        return Optional.ofNullable(transactionalOptions);
    }
}
