package ru.yandex.yt.ytclient.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.TransactionalClient;

/**
 * Spec of the reduce operation.
 */
@NonNullApi
@NonNullFields
public class ReduceSpec extends SimpleUserOperationSpecBase implements Spec {
    private final UserJobSpec reducerSpec;
    private final List<String> reduceBy;
    private final List<String> joinBy;

    private final @Nullable JobIo jobIo;
    private final boolean enableKeyGuarantee;

    public ReduceSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            List<String> reduceBy,
            String command) {
        this(inputTables, outputTables, reduceBy, new CommandSpec(command));
    }

    public ReduceSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            List<String> reduceBy,
            Reducer<?, ?> reducer) {
        this(inputTables, outputTables, reduceBy, new ReducerSpec(reducer));
    }

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

        if (reducerSpec instanceof MapperOrReducerSpec &&
                ((MapperOrReducerSpec) reducerSpec).trackIndices()) {
            jobIo = (builder.jobIo == null ? JobIo.builder() : builder.jobIo.toBuilder())
                    .setEnableRowIndex(true)
                    .setEnableTableIndex(true)
                    .build();
        } else {
            jobIo = builder.jobIo;
        }
        enableKeyGuarantee = builder.enableKeyGuarantee;
    }

    public Optional<JobIo> getJobIo() {
        return Optional.ofNullable(jobIo);
    }

    public List<String> getReduceBy() {
        return reduceBy;
    }

    public UserJobSpec getReducerSpec() {
        return reducerSpec;
    }

    public List<String> getJoinBy() {
        return joinBy;
    }

    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context) {
        SpecUtils.createOutputTables(yt, getOutputTables(), getOutputTableAttributes());
        return builder.beginMap()
                .apply(b -> SpecUtils.addMapperOrReducerTitle(b, reducerSpec))
                .key("reducer").apply(b -> reducerSpec.prepare(b, yt, context, getOutputTables().size()))
                .key("reduce_by").value(reduceBy)
                .when(!joinBy.isEmpty(), b -> b.key("join_by").value(joinBy))
                .when(jobIo != null, b -> b.key("job_io").value(jobIo.prepare()))
                .when(!enableKeyGuarantee, b -> b.key("enable_key_guarantee").value(false))
                .apply(b -> dumpToSpec(b, context))
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

    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> extends SimpleUserOperationSpecBase.Builder<T> {
        private @Nullable UserJobSpec reducerSpec;
        private @Nullable JobIo jobIo;
        private List<String> reduceBy = new ArrayList<>();
        private List<String> joinBy = new ArrayList<>();
        private boolean enableKeyGuarantee = true;

        protected BuilderBase() {
        }

        public ReduceSpec build() {
            return new ReduceSpec(this);
        }

        public T setReducerSpec(UserJobSpec reducerSpec) {
            this.reducerSpec = reducerSpec;
            return self();
        }

        public T setReducerCommand(String command) {
            return setReducerSpec(new CommandSpec(command));
        }

        public T setReduceBy(List<String> reduceBy) {
            this.reduceBy = new ArrayList<>(reduceBy);
            return self();
        }

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

        public T setJobIo(@Nullable JobIo jobIo) {
            this.jobIo = jobIo;
            return self();
        }

        /**
         * When this option is set to false and joinBy is specified,
         * Reduce operation behaves like JoinReduce.
         * <p>
         * <a href="https://docs.yandex-team.ru/yt/description/mr/reduce#foreign_tables">Documentation.</a>
         * <a href="https://clubs.at.yandex-team.ru/yt/3587">Blog post.</a>
         */
        public T setEnableKeyGuarantee(boolean enableKeyGuarantee) {
            this.enableKeyGuarantee = enableKeyGuarantee;
            return self();
        }
    }
}
