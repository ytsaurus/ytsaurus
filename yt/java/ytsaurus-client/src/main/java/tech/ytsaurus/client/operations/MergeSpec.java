package tech.ytsaurus.client.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Immutable spec for merge operation.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/merge">
 * merge documentation
 * </a>
 */
@NonNullApi
@NonNullFields
public class MergeSpec extends SystemOperationSpecBase implements Spec {
    @Nullable
    private final Integer jobCount;
    private final MergeMode mergeMode;
    private final boolean combineChunks;

    private final List<String> mergeBy;
    @Nullable
    private final DataSize dataSizePerJob;
    @Nullable
    private final DataSize maxDataSizePerJob;
    @Nullable
    private final JobIo jobIo;

    /**
     * Create merge spec from input tables and output table.
     */
    public MergeSpec(List<YPath> inputTables, YPath outputTable) {
        this(builder().setInputTables(inputTables).setOutputTable(outputTable));
    }

    protected <T extends BuilderBase<T>> MergeSpec(BuilderBase<T> builder) {
        super(builder);
        jobCount = builder.jobCount;
        mergeMode = builder.mergeMode;
        combineChunks = builder.combineChunks;
        mergeBy = builder.mergeBy;
        dataSizePerJob = builder.dataSizePerJob;
        jobIo = builder.jobIo;
        maxDataSizePerJob = builder.maxDataSizePerJob;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        MergeSpec spec = (MergeSpec) obj;

        return super.equals(obj)
                && Optional.ofNullable(jobCount).equals(Optional.ofNullable(spec.jobCount))
                && mergeMode.equals(spec.mergeMode)
                && combineChunks == spec.combineChunks
                && mergeBy.equals(spec.mergeBy)
                && Optional.ofNullable(dataSizePerJob).equals(Optional.ofNullable(spec.dataSizePerJob))
                && Optional.ofNullable(jobIo).equals(Optional.ofNullable(spec.jobIo))
                && Optional.ofNullable(maxDataSizePerJob).equals(Optional.ofNullable(spec.maxDataSizePerJob));
    }

    /**
     * @see Builder#setJobCount(Integer)
     */
    public Optional<Integer> getJobCount() {
        return Optional.ofNullable(jobCount);
    }

    /**
     * @see Builder#setDataSizePerJob(DataSize)
     */
    public Optional<DataSize> getDataSizePerJob() {
        return Optional.ofNullable(dataSizePerJob);
    }

    /**
     * @see Builder#setMergeMode
     */
    public MergeMode getMergeMode() {
        return mergeMode;
    }

    /**
     * @see Builder#setCombineChunks(boolean)
     */
    public boolean isCombineChunks() {
        return combineChunks;
    }

    /**
     * @see Builder#setMergeBy(String...)
     */
    public List<String> getMergeBy() {
        return mergeBy;
    }

    /**
     * @see Builder#setMaxDataSizePerJob(DataSize)
     */
    public Optional<DataSize> getMaxDataSizePerJob() {
        return Optional.ofNullable(maxDataSizePerJob);
    }

    /**
     * @see Builder#setJobIo(JobIo)
     */
    public Optional<JobIo> getJobIo() {
        return Optional.ofNullable(jobIo);
    }

    /**
     * Create output table if it doesn't exist and convert spec to yson.
     */
    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt,
                                SpecPreparationContext specPreparationContext) {
        SpecUtils.createOutputTables(
                yt,
                specPreparationContext.getTransactionalOptions().orElse(null),
                List.of(getOutputTable()),
                getOutputTableAttributes()
        );

        return builder.beginMap()
                .when(jobCount != null, b -> b.key("job_count").value(jobCount))
                .key("mode").value(mergeMode.value())
                .key("combine_chunks").value(combineChunks)
                .when(!mergeBy.isEmpty(), b -> b.key("merge_by").value(mergeBy))
                .when(dataSizePerJob != null, b -> b.key("data_size_per_job")
                        .value(Objects.requireNonNull(dataSizePerJob).toBytes()))
                .when(maxDataSizePerJob != null, b -> b.key("max_data_size_per_job")
                        .value(Objects.requireNonNull(maxDataSizePerJob).toBytes()))
                .when(jobIo != null, b -> b.key("job_io").value(Objects.requireNonNull(jobIo).prepare()))
                .apply(b -> toTree(b, specPreparationContext))
                .endMap();
    }

    /**
     * Create empty builder of {@link MergeSpec}.
     */
    public static BuilderBase<?> builder() {
        return new Builder();
    }

    /**
     * Builder of {@link MergeSpec}.
     */
    protected static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    // BuilderBase was taken out because there is another client
    // which we need to support too and which use the same MergeSpec class.
    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> extends SystemOperationSpecBase.Builder<T> {
        @Nullable
        private Integer jobCount;
        private MergeMode mergeMode = MergeMode.UNORDERED;
        private boolean combineChunks = false;

        private List<String> mergeBy = new ArrayList<>();
        @Nullable
        private DataSize dataSizePerJob;
        @Nullable
        private JobIo jobIo;
        @Nullable
        private DataSize maxDataSizePerJob;

        /**
         * Create instance of {@link MergeSpec}.
         */
        public MergeSpec build() {
            return new MergeSpec(this);
        }

        /**
         * Set how many jobs should be run. It is more prior than dataSizePerJob option.
         * It is advisory option.
         * There is a guarantee that if jobCount less or equal total input row count than job count will be exactly `jobCount`
         * if it does not contradict the restriction on the maximum number of jobs in the operation.
         */
        public T setJobCount(@Nullable Integer jobCount) {
            this.jobCount = jobCount;
            return self();
        }

        /**
         * Set data size per job.
         * It is advisory option.
         *
         * @see SimpleUserOperationSpecBase.Builder#setJobCount
         */
        public T setDataSizePerJob(DataSize dataSizePerJob) {
            this.dataSizePerJob = dataSizePerJob;
            return self();
        }

        /**
         * Set merge operation mode, can take the following values: unordered, sorted, ordered.
         */
        public T setMergeMode(MergeMode mergeMode) {
            this.mergeMode = mergeMode;
            return self();
        }

        /**
         * Set a setting that activates the enlargement of chunks.
         * By default, false.
         */
        public T setCombineChunks(boolean combineChunks) {
            this.combineChunks = combineChunks;
            return self();
        }

        /**
         * Set a list of columns by which sorted merge should be performed.
         */
        public T setMergeBy(Collection<String> mergeBy) {
            this.mergeBy = new ArrayList<>(mergeBy);
            return self();
        }

        /**
         * Set a list of columns by which sorted merge should be performed.
         */
        public T setMergeBy(String... mergeBy) {
            return setMergeBy(Arrays.asList(mergeBy));
        }

        /**
         * Set maximum allowed size of input data for one job.
         * This option sets a hard upper bound on the size of the job.
         * If the scheduler fails to generate a smaller job, the operation will fail.
         */
        public T setMaxDataSizePerJob(DataSize maxDataSizePerJob) {
            this.maxDataSizePerJob = maxDataSizePerJob;
            return self();
        }

        /**
         * Set job I/O options.
         *
         * @see JobIo
         */
        public T setJobIo(JobIo jobIo) {
            this.jobIo = jobIo;
            return self();
        }
    }
}
