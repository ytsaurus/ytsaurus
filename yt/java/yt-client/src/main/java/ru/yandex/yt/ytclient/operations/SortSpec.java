package ru.yandex.yt.ytclient.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.inside.yt.kosher.common.DataSize;
import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.TransactionalClient;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.request.CreateNode;
import ru.yandex.yt.ytclient.tables.SortColumn;

@NonNullApi
@NonNullFields
public class SortSpec extends SystemOperationSpecBase implements Spec {
    private final List<SortColumn> sortBy;

    private final @Nullable Integer partitionCount;
    private final @Nullable Integer partitionJobCount;
    private final @Nullable Integer sortJobCount;
    private final @Nullable
    DataSize maxDataSizePerSortJob;
    private final @Nullable DataSize maxDataSizePerUnorderedMergeJob;
    private final @Nullable JobIo mergeJobIo;

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
        sortJobCount = builder.sortJobCount;
        maxDataSizePerSortJob = builder.maxDataSizePerSortJob;
        maxDataSizePerUnorderedMergeJob = builder.maxDataSizePerUnorderedMergeJob;
        mergeJobIo = builder.mergeJobIo;
    }

    public Optional<Integer> getPartitionCount() {
        return Optional.ofNullable(partitionCount);
    }

    public Optional<Integer> getPartitionJobCount() {
        return Optional.ofNullable(partitionJobCount);
    }

    public Optional<Integer> getSortJobCount() {
        return Optional.ofNullable(sortJobCount);
    }

    public Optional<DataSize> getMaxDataSizePerSortJob() {
        return Optional.ofNullable(maxDataSizePerSortJob);
    }

    public Optional<DataSize> getMaxDataSizePerUnorderedMergeJob() {
        return Optional.ofNullable(maxDataSizePerUnorderedMergeJob);
    }

    public Optional<JobIo> getMergeJobIo() {
        return Optional.ofNullable(mergeJobIo);
    }

    public List<String> getSortBy() {
        return sortBy.stream().map(SortColumn::getName).collect(Collectors.toList());
    }

    public List<SortColumn> getSortByColumns() {
        return sortBy;
    }

    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context) {
        yt.createNode(CreateNode.builder()
                .setPath(getOutputTable())
                .setType(ObjectType.Table)
                .setAttributes(getOutputTableAttributes())
                .setRecursive(true)
                .setIgnoreExisting(true)
                .build());

        return builder.beginMap()
                .when(partitionCount != null, b -> b.key("partition_count").value(partitionCount))
                .when(partitionJobCount != null, b -> b.key("partition_job_count").value(partitionJobCount))
                .when(sortJobCount != null, b -> b.key("sort_job_count").value(sortJobCount))
                .when(maxDataSizePerSortJob != null, b -> b.key("max_data_size_per_sort_job")
                        .value(maxDataSizePerSortJob.toBytes()))
                .when(maxDataSizePerUnorderedMergeJob != null, b -> b.key("max_data_size_per_unordered_merge_job")
                        .value(maxDataSizePerUnorderedMergeJob.toBytes()))
                .key("sort_by").value(sortBy, (b, t) -> t.toTree(b))
                .when(mergeJobIo != null, b -> b.key("merge_job_io").value(mergeJobIo.prepare()))
                .apply(b -> toTree(b, context))
                .endMap();
    }

    public static BuilderBase<?> builder() {
        return new Builder();
    }

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
        List<SortColumn> sortBy = new ArrayList<>();
        @Nullable Integer partitionCount;
        @Nullable Integer partitionJobCount;
        @Nullable Integer sortJobCount;
        @Nullable DataSize maxDataSizePerSortJob;
        @Nullable DataSize maxDataSizePerUnorderedMergeJob;
        @Nullable JobIo mergeJobIo;

        public SortSpec build() {
            return new SortSpec(this);
        }

        public T setSortByColumns(List<SortColumn> sortBy) {
            this.sortBy = new ArrayList<>(sortBy);
            return self();
        }

        public T setSortByColumns(SortColumn... sortBy) {
            return setSortByColumns(Arrays.asList(sortBy));
        }

        public T setSortBy(Collection<String> sortBy) {
            return setSortByColumns(SortColumn.convert(sortBy));
        }

        public T setSortBy(String... sortBy) {
            return setSortBy(Arrays.asList(sortBy));
        }

        public T setPartitionCount(@Nullable Integer partitionCount) {
            this.partitionCount = partitionCount;
            return self();
        }

        public T setPartitionJobCount(@Nullable Integer partitionJobCount) {
            this.partitionJobCount = partitionJobCount;
            return self();
        }

        public T setSortJobCount(@Nullable Integer sortJobCount) {
            this.sortJobCount = sortJobCount;
            return self();
        }

        public T setMaxDataSizePerSortJob(@Nullable DataSize maxDataSizePerSortJob) {
            this.maxDataSizePerSortJob = maxDataSizePerSortJob;
            return self();
        }

        public T setMaxDataSizePerUnorderedMergeJob(@Nullable DataSize maxDataSizePerUnorderedMergeJob) {
            this.maxDataSizePerUnorderedMergeJob = maxDataSizePerUnorderedMergeJob;
            return self();
        }

        public T setMergeJobIo(@Nullable JobIo mergeJobIo) {
            this.mergeJobIo = mergeJobIo;
            return self();
        }
    }
}
