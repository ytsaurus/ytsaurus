package tech.ytsaurus.client.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Spec of the reduce operation.
 *
 * @see <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce">
 * reduce documentation
 * </a>
 */
@NonNullApi
@NonNullFields
public class ReduceSpec extends SimpleUserOperationSpecBase implements Spec {
    private final UserJobSpec reducerSpec;
    private final List<String> reduceBy;
    private final List<String> joinBy;

    private final @Nullable
    JobIo jobIo;
    private final boolean enableKeyGuarantee;

    /**
     * Construct reduce spec from input and output tables, `reduceBy` list of columns
     * and command with other options set by defaults.
     */
    public ReduceSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            List<String> reduceBy,
            String command) {
        this(inputTables, outputTables, reduceBy, new CommandSpec(command));
    }

    /**
     * Construct reduce spec from input and output tables, `reduceBy` list of columns
     * and reducer with other options set by defaults.
     */
    public ReduceSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            List<String> reduceBy,
            Reducer<?, ?> reducer) {
        this(inputTables, outputTables, reduceBy, new ReducerSpec(reducer));
    }

    /**
     * Construct reduce spec from input and output tables, `reduceBy` list of columns
     * and reducer spec with other options set by defaults.
     */
    public ReduceSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            List<String> reduceBy,
            UserJobSpec reducerSpec) {
        this(
                null,
                null,
                inputTables,
                outputTables,
                reduceBy,
                reducerSpec
        );
    }

    public ReduceSpec(
            @Nullable Integer jobCount,
            @Nullable DataSize maxDataSizePerJob,
            List<YPath> inputTables,
            List<YPath> outputTables,
            List<String> reduceBy,
            UserJobSpec reducerSpec) {
        this(
                builder()
                        .setJobCount(jobCount)
                        .setMaxDataSizePerJob(maxDataSizePerJob)
                        .setInputTables(inputTables)
                        .setOutputTables(outputTables)
                        .setReduceBy(reduceBy)
                        .setReducerSpec(reducerSpec)
        );
    }

    protected ReduceSpec(BuilderBase<?> builder) {
        super(builder);
        if (builder.reducerSpec == null) {
            throw new RuntimeException("reducer is not set");
        }
        reducerSpec = builder.reducerSpec;

        if (builder.joinBy.isEmpty() && builder.reduceBy.isEmpty()) {
            throw new RuntimeException("Neither reduceBy nor joinBy is set");
        }
        reduceBy = builder.reduceBy;
        joinBy = builder.joinBy;

        if (reducerSpec instanceof MapperOrReducerSpec) {
            MapperOrReducerSpec mapperOrReducerSpec = (MapperOrReducerSpec) reducerSpec;
            jobIo = mapperOrReducerSpec.createJobIo(builder.jobIo);
            if (mapperOrReducerSpec.mapperOrReducer.outputType().getClass() == EntityTableEntryType.class) {
                var outputTableSchema = ((EntityTableEntryType<?>) mapperOrReducerSpec
                        .mapperOrReducer.outputType()).getTableSchema();
                getOutputTables().replaceAll(yPath -> yPath.withSchema(outputTableSchema.toYTree()));
            }
        } else {
            jobIo = builder.jobIo;
        }
        enableKeyGuarantee = builder.enableKeyGuarantee;
    }

    /**
     * @see Builder#setJobIo(JobIo)
     */
    public Optional<JobIo> getJobIo() {
        return Optional.ofNullable(jobIo);
    }

    /**
     * @see Builder#setReduceBy(List)
     */
    public List<String> getReduceBy() {
        return reduceBy;
    }

    /**
     * @see Builder#setReducerSpec(UserJobSpec)
     */
    public UserJobSpec getReducerSpec() {
        return reducerSpec;
    }

    /**
     * @see Builder#setJoinBy(List)
     */
    public List<String> getJoinBy() {
        return joinBy;
    }

    /**
     * Create yson reduce spec to transfer to YT.
     */
    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt,
                                SpecPreparationContext specPreparationContext) {
        SpecUtils.createOutputTables(
                yt,
                specPreparationContext.getTransactionalOptions().orElse(null),
                getOutputTables(),
                getOutputTableAttributes()
        );

        var formatContext = FormatContext.builder()
                .setInputTableCount(getInputTables().size())
                .setOutputTableCount(getOutputTables().size())
                .build();
        return builder.beginMap()
                .apply(b -> SpecUtils.addMapperOrReducerTitle(b, reducerSpec))
                .key("reducer").apply(b -> reducerSpec.prepare(b, yt, specPreparationContext, formatContext))
                .key("reduce_by").value(reduceBy)
                .when(!joinBy.isEmpty(), b -> b.key("join_by").value(joinBy))
                .when(jobIo != null, b -> b.key("job_io").value(jobIo.prepare()))
                .when(!enableKeyGuarantee, b -> b.key("enable_key_guarantee").value(false))
                .apply(b -> dumpToSpec(b, specPreparationContext))
                .endMap();
    }

    /**
     * Construct empty builder for reduce spec.
     */
    public static BuilderBase<?> builder() {
        return new Builder();
    }

    /**
     * Builder for {@link ReduceSpec}
     */
    protected static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    /**
     * BuilderBase was taken out because there is another client
     * which we need to support too and which use the same ReduceSpec class.
     */
    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> extends SimpleUserOperationSpecBase.Builder<T> {
        @Nullable
        private UserJobSpec reducerSpec;
        @Nullable
        private JobIo jobIo;
        private List<String> reduceBy = new ArrayList<>();
        private List<String> joinBy = new ArrayList<>();
        private boolean enableKeyGuarantee = true;

        protected BuilderBase() {
        }

        /**
         * Construct {@link ReduceSpec} instance.
         */
        public ReduceSpec build() {
            return new ReduceSpec(this);
        }

        /**
         * Set reducer spec.
         *
         * @see ReducerSpec
         * @see CommandSpec
         */
        public T setReducerSpec(UserJobSpec reducerSpec) {
            this.reducerSpec = reducerSpec;
            return self();
        }

        /**
         * Set reducer command.
         * Create CommandSpec as user job spec from command with other options set by defaults.
         */
        public T setReducerCommand(String command) {
            return setReducerSpec(new CommandSpec(command));
        }

        /**
         * Set a list of columns by which reduce is carried out;
         */
        public T setReduceBy(List<String> reduceBy) {
            this.reduceBy = new ArrayList<>(reduceBy);
            return self();
        }

        /**
         * Set a list of columns by which reduce is carried out;
         */
        public T setReduceBy(String... reduceBy) {
            return setReduceBy(Arrays.asList(reduceBy));
        }

        public T setJoinBy(List<String> joinBy) {
            this.joinBy = new ArrayList<>(joinBy);
            return self();
        }

        public T setJoinBy(String... joinBy) {
            return setJoinBy(Arrays.asList(joinBy));
        }

        /**
         * Set job I/O options.
         *
         * @see JobIo
         */
        public T setJobIo(@Nullable JobIo jobIo) {
            this.jobIo = jobIo;
            return self();
        }

        /**
         * When this option is set to false and joinBy is specified,
         * Reduce operation behaves like JoinReduce.
         * <p>
         * <a href="https://ytsaurus.tech/docs/en/user-guide/data-processing/operations/reduce#foreign_tables">Documentation.</a>
         */
        public T setEnableKeyGuarantee(boolean enableKeyGuarantee) {
            this.enableKeyGuarantee = enableKeyGuarantee;
            return self();
        }
    }
}
