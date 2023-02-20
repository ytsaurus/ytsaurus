package tech.ytsaurus.client.operations;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Base class for map and reduce operations spec
 */
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

    /**
     * @see Builder#setJobCount(Integer)
     */
    public Optional<Integer> getJobCount() {
        return Optional.ofNullable(jobCount);
    }

    /**
     * @see Builder#setMaxDataSizePerJob(DataSize)
     */
    public Optional<DataSize> getMaxDataSizePerJob() {
        return Optional.ofNullable(maxDataSizePerJob);
    }

    /**
     * @see Builder#setDataSizePerJob(DataSize)
     */
    public Optional<DataSize> getDataSizePerJob() {
        return Optional.ofNullable(dataSizePerJob);
    }

    /**
     * Convert to yson.
     */
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

        /**
         * Set how many jobs should be run. It is more prior than dataSizePerJob option.
         * It is advisory option.
         * There is a guarantee that if jobCount <= total input row count than job count will be exactly `jobCount`
         * if it does not contradict the restriction on the maximum number of jobs in the operation.
         */
        public T setJobCount(@Nullable Integer jobCount) {
            this.jobCount = jobCount;
            return self();
        }

        /**
         * Set maximum allowed size of input data for one job.
         * This option sets a hard upper bound on the size of the job.
         * If the scheduler fails to generate a smaller job, the operation will fail.
         */
        public T setMaxDataSizePerJob(@Nullable DataSize maxDataSizePerJob) {
            this.maxDataSizePerJob = maxDataSizePerJob;
            return self();
        }

        /**
         * Set data size per job.
         * It is advisory option.
         *
         * @see Builder#setJobCount
         */
        public T setDataSizePerJob(@Nullable DataSize dataSizePerJob) {
            this.dataSizePerJob = dataSizePerJob;
            return self();
        }
    }
}
