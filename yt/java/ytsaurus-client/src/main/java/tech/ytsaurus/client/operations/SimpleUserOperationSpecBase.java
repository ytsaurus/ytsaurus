package tech.ytsaurus.client.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

// Base class for map and reduce operations spec
@NonNullApi
@NonNullFields
public class SimpleUserOperationSpecBase extends UserOperationSpecBase {
    private final @Nullable
    Integer jobCount;
    private final @Nullable
    DataSize maxDataSizePerJob;
    private final @Nullable
    DataSize dataSizePerJob;

    protected SimpleUserOperationSpecBase(Builder<?> builder) {
        super(builder);

        jobCount = builder.jobCount;
        dataSizePerJob = builder.dataSizePerJob;
        maxDataSizePerJob = builder.maxDataSizePerJob;
    }

    public Optional<Integer> getJobCount() {
        return Optional.ofNullable(jobCount);
    }

    public Optional<DataSize> getMaxDataSizePerJob() {
        return Optional.ofNullable(maxDataSizePerJob);
    }

    public Optional<DataSize> getDataSizePerJob() {
        return Optional.ofNullable(dataSizePerJob);
    }

    protected YTreeBuilder dumpToSpec(YTreeBuilder mapBuilder, SpecPreparationContext context) {
        return mapBuilder
                .key("started_by").apply(b -> SpecUtils.startedBy(b, context))
                .when(jobCount != null, b -> b.key("job_count").value(jobCount))
                .when(maxDataSizePerJob != null,
                        b -> b.key("max_data_size_per_job").value(maxDataSizePerJob.toBytes()))
                .when(dataSizePerJob != null,
                        b -> b.key("data_size_per_job").value(dataSizePerJob.toBytes()))
                .apply(b -> toTree(b, context));
    }

    @NonNullApi
    @NonNullFields
    protected abstract static class Builder<T extends Builder<T>> extends UserOperationSpecBase.Builder<T> {
        private @Nullable
        Integer jobCount;
        private @Nullable
        DataSize maxDataSizePerJob;
        private @Nullable
        DataSize dataSizePerJob;

        public T setJobCount(@Nullable Integer jobCount) {
            this.jobCount = jobCount;
            return self();
        }

        public T setMaxDataSizePerJob(@Nullable DataSize maxDataSizePerJob) {
            this.maxDataSizePerJob = maxDataSizePerJob;
            return self();
        }

        public T setDataSizePerJob(@Nullable DataSize dataSizePerJob) {
            this.dataSizePerJob = dataSizePerJob;
            return self();
        }
    }
}
