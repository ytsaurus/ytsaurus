package ru.yandex.yt.ytclient.operations;

import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeBuilder;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

/**
 * Spec of the map operation.
 */
@NonNullApi
@NonNullFields
public class MapSpec extends SimpleUserOperationSpecBase implements Spec {
    private final UserJobSpec mapperSpec;
    private final @Nullable JobIo jobIo;

    public MapSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            String command) {
        this(builder()
                .setInputTables(inputTables)
                .setOutputTables(outputTables)
                .setMapperCommand(command));
    }

    public MapSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            Mapper<?, ?> mapper) {
        this(builder()
                .setInputTables(inputTables)
                .setOutputTables(outputTables)
                .setMapperSpec(new MapperSpec(mapper)));
    }

    public MapSpec(
            List<YPath> inputTables,
            List<YPath> outputTables,
            UserJobSpec mapperSpec) {
        this(builder()
                .setInputTables(inputTables)
                .setOutputTables(outputTables)
                .setMapperSpec(mapperSpec));
    }

    protected <T extends BuilderBase<T>> MapSpec(BuilderBase<T> builder) {
        super(builder);

        if (builder.mapperSpec == null) {
            throw new RuntimeException("mapper is not set");
        }
        mapperSpec = builder.mapperSpec;

        if (mapperSpec instanceof MapperOrReducerSpec && ((MapperOrReducerSpec) mapperSpec).trackIndices()) {
            this.jobIo = (builder.jobIo == null ? JobIo.builder() : builder.jobIo.toBuilder())
                    .setEnableRowIndex(true)
                    .setEnableTableIndex(true)
                    .build();
        } else {
            this.jobIo = builder.jobIo;
        }
    }

    public Optional<JobIo> getJobIo() {
        return Optional.ofNullable(jobIo);
    }

    public UserJobSpec getMapperSpec() {
        return mapperSpec;
    }

    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context) {
        SpecUtils.createOutputTables(yt, getOutputTables(), getOutputTableAttributes());
        return builder.beginMap()
                .apply(b -> SpecUtils.addMapperOrReducerTitle(b, mapperSpec))
                .key("mapper").apply(b -> mapperSpec.prepare(b, yt, context, getOutputTables().size()))
                .when(jobIo != null, b -> b.key("job_io").value(jobIo.prepare()))
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
        private @Nullable UserJobSpec mapperSpec;
        private @Nullable JobIo jobIo;

        protected BuilderBase() {
        }

        public MapSpec build() {
            return new MapSpec(this);
        }

        public T setMapperSpec(UserJobSpec mapperSpec) {
            this.mapperSpec = mapperSpec;
            return self();
        }

        public T setMapperCommand(String command) {
            return setMapperSpec(new CommandSpec(command));
        }

        public T setJobIo(@Nullable JobIo jobIo) {
            this.jobIo = jobIo;
            return self();
        }
    }
}
