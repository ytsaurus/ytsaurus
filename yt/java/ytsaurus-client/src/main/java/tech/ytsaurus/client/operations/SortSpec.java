package tech.ytsaurus.client.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.core.tables.SortColumn;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Immutable sort spec.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/sort">
 * sort documentation
 * </a>
 */
@NonNullApi
@NonNullFields
public class SortSpec extends SystemOperationSpecBase implements Spec {
    private final List<SortColumn> sortBy;

    @Nullable
    private final Integer partitionCount;
    @Nullable
    private final Integer partitionJobCount;
    @Nullable
    private final DataSize dataSizePerSortJob;
    @Nullable
    private final DataSize dataSizePerSortedMergeJob;
    @Nullable
    private final JobIo mergeJobIo;

    /**
     * Create sort spec from input tables, output table and list of columns.
     */
    public SortSpec(
            List<YPath> inputTables,
            YPath outputTable,
            List<String> sortBy) {
        this(
                SortSpec.builder()
                        .setInputTables(inputTables)
                        .setOutputTable(outputTable)
                        .setSortBy(sortBy)
        );
    }

    protected <T extends BuilderBase<T>> SortSpec(BuilderBase<T> builder) {
        super(builder);
        if (builder.sortBy.isEmpty()) {
            throw new RuntimeException("sortBy is not specified");
        }
        sortBy = builder.sortBy;

        partitionCount = builder.partitionCount;
        partitionJobCount = builder.partitionJobCount;
        dataSizePerSortJob = builder.dataSizePerSortJob;
        dataSizePerSortedMergeJob = builder.dataSizePerSortedMergeJob;
        mergeJobIo = builder.mergeJobIo;
    }

    /**
     * @see Builder#setPartitionCount(Integer)
     */
    public Optional<Integer> getPartitionCount() {
        return Optional.ofNullable(partitionCount);
    }

    /**
     * @see Builder#setPartitionJobCount(Integer)
     */
    public Optional<Integer> getPartitionJobCount() {
        return Optional.ofNullable(partitionJobCount);
    }

    /**
     * @see Builder#setDataSizePerSortJob(DataSize)
     */
    public Optional<DataSize> getDataSizePerSortJob() {
        return Optional.ofNullable(dataSizePerSortJob);
    }

    /**
     * @see Builder#setDataSizePerSortedMergeJob(DataSize)
     */
    public Optional<DataSize> getDataSizePerSortedMergeJob() {
        return Optional.ofNullable(dataSizePerSortedMergeJob);
    }

    /**
     * @see Builder#setMergeJobIo(JobIo)
     */
    public Optional<JobIo> getMergeJobIo() {
        return Optional.ofNullable(mergeJobIo);
    }

    /**
     * @see Builder#setSortBy(String...)
     */
    public List<String> getSortBy() {
        return sortBy.stream().map(SortColumn::getName).collect(Collectors.toList());
    }

    /**
     * @see Builder#setSortByColumns(List)
     */
    public List<SortColumn> getSortByColumns() {
        return sortBy;
    }

    /**
     * Convert to yson.
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
                .when(partitionCount != null, b -> b.key("partition_count").value(partitionCount))
                .when(partitionJobCount != null, b -> b.key("partition_job_count").value(partitionJobCount))
                .when(dataSizePerSortJob != null, b -> b.key("data_size_per_sort_job")
                        .value(Objects.requireNonNull(dataSizePerSortJob).toBytes()))
                .when(dataSizePerSortedMergeJob != null, b -> b.key("data_size_per_sorted_merge_job")
                        .value(Objects.requireNonNull(dataSizePerSortedMergeJob).toBytes()))
                .key("sort_by").value(sortBy, (b, t) -> t.toTree(b))
                .when(mergeJobIo != null,
                        b -> b.key("merge_job_io").value(Objects.requireNonNull(mergeJobIo).prepare()))
                .apply(b -> toTree(b, specPreparationContext))
                .endMap();
    }

    /**
     * Create empty builder.
     */
    public static BuilderBase<?> builder() {
        return new Builder();
    }

    /**
     * Builder of {@link SortSpec}.
     */
    protected static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    // BuilderBase was taken out because there is another client
    // which we need to support too and which use the same SortSpec class.
    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> extends SystemOperationSpecBase.Builder<T> {
        private List<SortColumn> sortBy = new ArrayList<>();
        @Nullable
        private Integer partitionCount;
        @Nullable
        private Integer partitionJobCount;
        @Nullable
        private DataSize dataSizePerSortJob;
        @Nullable
        private DataSize dataSizePerSortedMergeJob;
        @Nullable
        private JobIo mergeJobIo;

        /**
         * Create instance of {@link SortSpec}.
         */
        public SortSpec build() {
            return new SortSpec(this);
        }

        /**
         * Set list of columns to sort by. It is required parameter.
         */
        public T setSortByColumns(List<SortColumn> sortBy) {
            this.sortBy = new ArrayList<>(sortBy);
            return self();
        }

        /**
         * Set list of columns to sort by. It is required parameter.
         */
        public T setSortByColumns(SortColumn... sortBy) {
            return setSortByColumns(Arrays.asList(sortBy));
        }

        /**
         * Set list of columns to sort by. It is required parameter.
         */
        public T setSortBy(Collection<String> sortBy) {
            return setSortByColumns(SortColumn.convert(sortBy));
        }

        /**
         * Set list of columns to sort by. It is required parameter.
         */
        public T setSortBy(String... sortBy) {
            return setSortBy(Arrays.asList(sortBy));
        }

        /**
         * Set how many partitions should be made in the sort. It is advisory.
         */
        public T setPartitionCount(@Nullable Integer partitionCount) {
            this.partitionCount = partitionCount;
            return self();
        }

        /**
         * Set how many partition jobs should be run. It is advisory.
         */
        public T setPartitionJobCount(@Nullable Integer partitionJobCount) {
            this.partitionJobCount = partitionJobCount;
            return self();
        }

        /**
         * Set recommended amount of input data for one sort job.
         */
        public T setDataSizePerSortJob(@Nullable DataSize dataSizePerSortJob) {
            this.dataSizePerSortJob = dataSizePerSortJob;
            return self();
        }

        /**
         * Set recommended amount of input data for one sorted merge job.
         */
        public T setDataSizePerSortedMergeJob(@Nullable DataSize dataSizePerSortedMergeJob) {
            this.dataSizePerSortedMergeJob = dataSizePerSortedMergeJob;
            return self();
        }

        /**
         * Set job I/O options.
         */
        public T setMergeJobIo(@Nullable JobIo mergeJobIo) {
            this.mergeJobIo = mergeJobIo;
            return self();
        }
    }
}
