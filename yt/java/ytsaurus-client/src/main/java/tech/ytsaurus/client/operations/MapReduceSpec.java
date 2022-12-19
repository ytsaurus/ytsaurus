package tech.ytsaurus.client.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.tables.SortColumn;
import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class MapReduceSpec extends UserOperationSpecBase implements Spec {
    private final List<String> reduceBy;
    private final List<SortColumn> sortBy;

    private final @Nullable
    UserJobSpec mapperSpec;
    private final @Nullable
    UserJobSpec reduceCombinerSpec;
    private final UserJobSpec reducerSpec;

    private final @Nullable
    Integer mapJobCount;
    private final @Nullable
    Integer partitionCount;
    private final @Nullable
    Integer partitionJobCount;
    private final @Nullable
    Integer sortJobCount;

    private final @Nullable
    DataSize dataSizePerSortJob;
    private final @Nullable
    DataSize maxDataSizePerSortJob;
    private final @Nullable
    Integer mapperOutputTableCount;

    private final @Nullable
    JobIo mapJobIo;
    private final @Nullable
    JobIo sortJobIo;
    private final @Nullable
    JobIo reduceJobIo;

    protected <T extends BuilderBase<T>> MapReduceSpec(BuilderBase<T> builder) {
        super(builder);

        reduceBy = builder.reduceBy;
        sortBy = builder.sortBy;
        mapperSpec = builder.mapperSpec;
        reduceCombinerSpec = builder.reduceCombinerSpec;

        if (builder.reducerSpec == null) {
            throw new RuntimeException("reducerSpec is not specified");
        }
        reducerSpec = builder.reducerSpec;

        mapJobCount = builder.mapJobCount;
        partitionCount = builder.partitionCount;
        partitionJobCount = builder.partitionJobCount;
        sortJobCount = builder.sortJobCount;

        dataSizePerSortJob = builder.dataSizePerSortJob;
        maxDataSizePerSortJob = builder.maxDataSizePerSortJob;
        mapperOutputTableCount = builder.mapperOutputTableCount;

        mapJobIo = builder.mapJobIo;
        sortJobIo = builder.sortJobIo;
        reduceJobIo = builder.reduceJobIo;
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

        MapReduceSpec spec = (MapReduceSpec) obj;
        return reduceBy.equals(spec.reduceBy)
                && sortBy.equals(spec.sortBy)
                && Optional.ofNullable(mapperSpec).equals(Optional.ofNullable(spec.mapperSpec))
                && Optional.ofNullable(reduceCombinerSpec).equals(Optional.ofNullable(spec.reduceCombinerSpec))
                && reducerSpec.equals(spec.reducerSpec)
                && Optional.ofNullable(mapJobCount).equals(Optional.ofNullable(spec.mapJobCount))
                && Optional.ofNullable(partitionCount).equals(Optional.ofNullable(spec.partitionCount))
                && Optional.ofNullable(partitionJobCount).equals(Optional.ofNullable(spec.partitionJobCount))
                && Optional.ofNullable(sortJobCount).equals(Optional.ofNullable(spec.sortJobCount))
                && Optional.ofNullable(dataSizePerSortJob).equals(Optional.ofNullable(spec.dataSizePerSortJob))
                && Optional.ofNullable(maxDataSizePerSortJob).equals(Optional.ofNullable(spec.maxDataSizePerSortJob))
                && Optional.ofNullable(mapperOutputTableCount).equals(Optional.ofNullable(spec.mapperOutputTableCount))
                && Optional.ofNullable(mapJobIo).equals(Optional.ofNullable(spec.mapJobIo))
                && Optional.ofNullable(sortJobIo).equals(Optional.ofNullable(spec.sortJobIo))
                && Optional.ofNullable(reduceJobIo).equals(Optional.ofNullable(spec.reduceJobIo));
    }

    public Optional<Integer> getMapJobCount() {
        return Optional.ofNullable(mapJobCount);
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

    public Optional<DataSize> getDataSizePerSortJob() {
        return Optional.ofNullable(dataSizePerSortJob);
    }

    public Optional<DataSize> getMaxDataSizePerSortJob() {
        return Optional.ofNullable(maxDataSizePerSortJob);
    }

    public Optional<Integer> getMapperOutputTableCount() {
        return Optional.ofNullable(mapperOutputTableCount);
    }

    public Optional<UserJobSpec> getMapperSpec() {
        return Optional.ofNullable(mapperSpec);
    }

    public List<String> getSortBy() {
        return sortBy.stream().map(SortColumn::getName).collect(Collectors.toList());
    }

    public List<SortColumn> getSortByColumns() {
        return sortBy;
    }

    public List<String> getReduceBy() {
        return reduceBy;
    }

    public Optional<UserJobSpec> getReduceCombinerSpec() {
        return Optional.ofNullable(reduceCombinerSpec);
    }

    public UserJobSpec getReducerSpec() {
        return reducerSpec;
    }

    public Optional<JobIo> getMapJobIo() {
        return Optional.ofNullable(mapJobIo);
    }

    public Optional<JobIo> getSortJobIo() {
        return Optional.ofNullable(sortJobIo);
    }

    public Optional<JobIo> getReduceJobIo() {
        return Optional.ofNullable(reduceJobIo);
    }

    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context) {
        SpecUtils.createOutputTables(yt, getOutputTables(), getOutputTableAttributes());
        @Nullable final String title;
        List<Title> titles = new ArrayList<>();
        if (mapperSpec != null) {
            Optional<String> mapperTitle = SpecUtils.getMapperOrReducerTitle(mapperSpec);
            mapperTitle.ifPresent(s -> titles.add(new Title("mapper", s)));
        }
        Optional<String> reducerTitle = SpecUtils.getMapperOrReducerTitle(reducerSpec);
        reducerTitle.ifPresent(s -> titles.add(new Title("reducer", s)));
        if (reduceCombinerSpec != null) {
            Optional<String> reduceCombinerTitle = SpecUtils.getMapperOrReducerTitle(reduceCombinerSpec);
            reduceCombinerTitle.ifPresent(s -> titles.add(new Title("reduce-combiner", s)));
        }
        if (titles.isEmpty()) {
            title = null;
        } else if (titles.size() == 1) {
            title = titles.get(0).title;
        } else {
            title = titles.stream().map(Title::toString).collect(Collectors.joining(", "));
        }

        int mapperOutputCount = 1 + Optional.ofNullable(mapperOutputTableCount).orElse(0);
        int reduceCombinerOutputCount = 1;
        int reducerOutputCount = getOutputTables().size() - Optional.ofNullable(mapperOutputTableCount).orElse(0);

        return builder.beginMap()
                .when(title != null, b -> b.key("title").value(title))
                .when(mapJobCount != null, b -> b.key("map_job_count").value(mapJobCount))
                .when(partitionCount != null, b -> b.key("partition_count").value(partitionCount))
                .when(partitionJobCount != null, b -> b.key("partition_job_count").value(partitionJobCount))
                .when(sortJobCount != null, b -> b.key("sort_job_count").value(sortJobCount))
                .when(dataSizePerSortJob != null, b -> b.key("data_size_per_sort_job")
                        .value(dataSizePerSortJob.toBytes()))
                .when(maxDataSizePerSortJob != null, b -> b.key("max_data_size_per_sort_job")
                        .value(maxDataSizePerSortJob.toBytes()))
                .when(mapperSpec != null, b -> b.key("mapper").apply(b2 ->
                        mapperSpec.prepare(b2, yt, context, mapperOutputCount)))
                .key("sort_by").value(sortBy, (b, t) -> t.toTree(b))
                .key("reduce_by").value(reduceBy)
                .key("reducer").apply(b -> reducerSpec.prepare(b, yt, context, reducerOutputCount))
                .when(reduceCombinerSpec != null, b -> b.key("reduce_combiner")
                        .apply(b2 -> reduceCombinerSpec.prepare(b2, yt, context, reduceCombinerOutputCount)))
                .key("started_by").apply(b -> SpecUtils.startedBy(b, context))
                .when(mapperOutputTableCount != null,
                        b -> b.key("mapper_output_table_count").value(mapperOutputTableCount))
                .when(mapJobIo != null, b -> b.key("map_job_io").value(mapJobIo.prepare()))
                .when(sortJobIo != null, b -> b.key("sort_job_io").value(sortJobIo.prepare()))
                .when(reduceJobIo != null, b -> b.key("reduce_job_io").value(reduceJobIo.prepare()))
                .apply(b -> toTree(b, context))
                .endMap();
    }

    public static BuilderBase<?> builder() {
        return new Builder();
    }

    @NonNullApi
    @NonNullFields
    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> extends UserOperationSpecBase.Builder<T> {
        private List<String> reduceBy = new ArrayList<>();
        private List<SortColumn> sortBy = new ArrayList<>();

        private @Nullable
        UserJobSpec mapperSpec;
        private @Nullable
        UserJobSpec reduceCombinerSpec;
        private @Nullable
        UserJobSpec reducerSpec;

        private @Nullable
        Integer mapJobCount;
        private @Nullable
        Integer partitionCount;
        private @Nullable
        Integer partitionJobCount;
        private @Nullable
        Integer sortJobCount;

        private @Nullable
        DataSize dataSizePerSortJob;
        private @Nullable
        DataSize maxDataSizePerSortJob;
        private @Nullable
        Integer mapperOutputTableCount;

        private @Nullable
        JobIo mapJobIo;
        private @Nullable
        JobIo sortJobIo;
        private @Nullable
        JobIo reduceJobIo;

        public MapReduceSpec build() {
            return new MapReduceSpec(this);
        }

        /**
         * Set a list of columns by which reduce is carried out;
         */
        public T setReduceBy(List<String> reduceBy) {
            this.reduceBy = new ArrayList<>(reduceBy);
            return self();
        }

        /**
         * @see Builder#setReduceBy(List)
         */
        public T setReduceBy(String... reduceBy) {
            return setReduceBy(Arrays.asList(reduceBy));
        }

        /**
         * Set a list of columns by which the input tables are to be sorted.
         * The option enables an additional check for sorting of input tables
         * and guarantees that rows are sorted by a given set of columns inside a user script.
         * The reduceBy sequence of columns must be a prefix of the sortBy sequence of columns.
         */
        public T setSortByColumns(List<SortColumn> sortBy) {
            this.sortBy = new ArrayList<>(sortBy);
            return self();
        }

        /**
         * @see Builder#setSortByColumns(List)
         */
        public T setSortByColumns(SortColumn... sortBy) {
            return setSortByColumns(Arrays.asList(sortBy));
        }

        /**
         * @see Builder#setSortByColumns(List)
         */
        public T setSortBy(List<String> sortBy) {
            return setSortByColumns(SortColumn.convert(sortBy));
        }

        /**
         * @see Builder#setSortByColumns(List)
         */
        public T setSortBy(String... sortBy) {
            return setSortBy(Arrays.asList(sortBy));
        }

        public T setMapperSpec(@Nullable UserJobSpec mapperSpec) {
            this.mapperSpec = mapperSpec;
            return self();
        }

        public T setMapperSpec(@Nullable MapperSpec mapperSpec) {
            this.mapperSpec = mapperSpec;
            return self();
        }

        public T setReduceCombinerSpec(@Nullable UserJobSpec reduceCombinerSpec) {
            this.reduceCombinerSpec = reduceCombinerSpec;
            return self();
        }

        public T setReduceCombinerSpec(@Nullable ReducerSpec reduceCombinerSpec) {
            this.reduceCombinerSpec = reduceCombinerSpec;
            return self();
        }

        public T setReducerSpec(UserJobSpec reducerSpec) {
            this.reducerSpec = reducerSpec;
            return self();
        }

        public T setReducerSpec(ReducerSpec reducerSpec) {
            this.reducerSpec = reducerSpec;
            return self();
        }

        public T setMapJobCount(@Nullable Integer mapJobCount) {
            this.mapJobCount = mapJobCount;
            return self();
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

        public T setDataSizePerSortJob(@Nullable DataSize dataSizePerSortJob) {
            this.dataSizePerSortJob = dataSizePerSortJob;
            return self();
        }

        public T setMaxDataSizePerSortJob(@Nullable DataSize maxDataSizePerSortJob) {
            this.maxDataSizePerSortJob = maxDataSizePerSortJob;
            return self();
        }

        public T setMapperOutputTableCount(@Nullable Integer mapperOutputTableCount) {
            this.mapperOutputTableCount = mapperOutputTableCount;
            return self();
        }

        public T setMapJobIo(@Nullable JobIo mapJobIo) {
            this.mapJobIo = mapJobIo;
            return self();
        }

        public T setSortJobIo(@Nullable JobIo sortJobIo) {
            this.sortJobIo = sortJobIo;
            return self();
        }

        public T setReduceJobIo(@Nullable JobIo reduceJobIo) {
            this.reduceJobIo = reduceJobIo;
            return self();
        }
    }

    private static class Title {
        final String name;
        final String title;

        Title(String name, String title) {
            this.name = name;
            this.title = title;
        }

        public String toString() {
            return this.name + ": " + this.title;
        }
    }
}
