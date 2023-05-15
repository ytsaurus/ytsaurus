package tech.ytsaurus.client.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.DataSize;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;


/**
 * Immutable command spec.
 */
@NonNullFields
@NonNullApi
public class CommandSpec implements Spec, UserJobSpec {
    private final String command;

    // These should be format description
    private final YTableEntryType<?> inputType;
    private final YTableEntryType<?> outputType;

    private final List<YPath> files;

    @Nullable
    private final DataSize memoryLimit;

    private final boolean useTmpfs;

    @Nullable
    private final DataSize tmpfsSize;

    private final Map<String, String> environment;
    @Nullable
    private final Double cpuLimit;

    @Nullable
    private final Long jobTimeLimit;

    // Only relevant for vanilla operations
    // (TODO: move to separate class VanillaCommandSpec)
    protected final List<YPath> outputTablePaths;
    private final @Nullable
    Integer jobCount;

    /**
     * Create command spec from command with other options set to defaults.
     */
    public CommandSpec(String command) {
        this(builder().setCommand(command));
    }

    protected <T extends BuilderBase<T>> CommandSpec(BuilderBase<T> builder) {
        if (builder.command == null) {
            throw new RuntimeException("command is not set");
        }
        command = builder.command;
        inputType = builder.inputType;
        outputType = builder.outputType;
        files = builder.files;
        memoryLimit = builder.memoryLimit;
        useTmpfs = builder.useTmpfs;
        tmpfsSize = builder.tmpfsSize;
        environment = builder.environment;
        cpuLimit = builder.cpuLimit;
        jobTimeLimit = builder.jobTimeLimit;
        jobCount = builder.jobCount;
        outputTablePaths = builder.outputTablePaths;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CommandSpec spec = (CommandSpec) obj;
        return command.equals(spec.command)
                && inputType.equals(spec.inputType)
                && outputType.equals(spec.outputType)
                && files.equals(spec.files)
                && Optional.ofNullable(memoryLimit).equals(Optional.ofNullable(spec.memoryLimit))
                && useTmpfs == spec.useTmpfs
                && Optional.ofNullable(tmpfsSize).equals(Optional.ofNullable(spec.tmpfsSize))
                && environment.equals(spec.environment)
                && Optional.ofNullable(cpuLimit).equals(Optional.ofNullable(spec.cpuLimit))
                && Optional.ofNullable(jobTimeLimit).equals(Optional.ofNullable(jobTimeLimit))
                && outputTablePaths.equals(spec.outputTablePaths)
                && Optional.ofNullable(jobCount).equals(Optional.ofNullable(spec.jobCount));
    }

    /**
     * @see Builder#setCommand(String)
     */
    public String getCommand() {
        return command;
    }

    /**
     * @see Builder#setInputType(YTableEntryType)
     */
    public YTableEntryType<?> getInputType() {
        return inputType;
    }

    /**
     * @see Builder#setOutputType(YTableEntryType)
     */
    public YTableEntryType<?> getOutputType() {
        return outputType;
    }

    /**
     * @see Builder#setFiles(List)
     */
    public List<YPath> getFiles() {
        return files;
    }

    /**
     * @see Builder#setMemoryLimit(DataSize)
     */
    public Optional<DataSize> getMemoryLimit() {
        return Optional.ofNullable(memoryLimit);
    }

    public boolean isUseTmpfs() {
        return useTmpfs;
    }

    public Optional<DataSize> getTmpfsSize() {
        return Optional.ofNullable(tmpfsSize);
    }

    /**
     * @see Builder#setEnvironment(Map)
     */
    public Map<String, String> getEnvironment() {
        return environment;
    }

    /**
     * @see Builder#setCpuLimit(Double)
     */
    public Optional<Double> getCpuLimit() {
        return Optional.ofNullable(cpuLimit);
    }

    /**
     * @see Builder#setJobTimeLimit(Long)
     */
    @SuppressWarnings("unused")
    public Optional<Long> getJobTimeLimit() {
        return Optional.ofNullable(jobTimeLimit);
    }

    /**
     * @see Builder#setJobCount(Integer)
     */
    public Optional<Integer> getJobCount() {
        return Optional.ofNullable(jobCount);
    }

    /**
     * Convert to yson.
     */
    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt,
                                SpecPreparationContext specPreparationContext) {
        var formatContext = FormatContext.builder()
                .setOutputTableCount(outputTablePaths.size())
                .build();
        return builder.beginMap()
                .key("command").value(command)
                .key("input_format").value(inputType.format(formatContext))
                .key("output_format").value(outputType.format(formatContext))
                .when(!files.isEmpty(), b -> b.key("file_paths").value(files, (b2, t) -> t.toTree(b2)))
                .when(memoryLimit != null, b -> b.key("memory_limit").value(memoryLimit.toBytes()))
                .when(useTmpfs, b -> b.key("tmpfs_path").value(".").key("copy_files").value(true))
                .when(tmpfsSize != null, b -> b.key("tmpfs_size").value(tmpfsSize.toBytes()))
                .key("environment").value(environment)
                .when(cpuLimit != null, b -> b.key("cpu_limit").value(cpuLimit))
                .when(jobTimeLimit != null, b -> b.key("job_time_limit").value(jobTimeLimit))
                .when(jobCount != null, b -> b.key("job_count").value(jobCount))
                .when(!outputTablePaths.isEmpty(), b -> b.key("output_table_paths").value(
                        outputTablePaths.stream().map(YPath::toTree).collect(Collectors.toList())
                ))
                .endMap();
    }

    /**
     * Convert to yson.
     */
    @Override
    public YTreeBuilder prepare(
            YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext specPreparationContext,
            FormatContext formatContext) {
        return prepare(builder, yt, specPreparationContext);
    }

    /**
     * Create empty builder.
     */
    public static BuilderBase<?> builder() {
        return new Builder();
    }

    /**
     * Builder of {@link CommandSpec}.
     */
    protected static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    // BuilderBase was taken out because there is another client
    // which we need to support too and which use the same CommandSpec class.
    @NonNullFields
    @NonNullApi
    public abstract static class BuilderBase<T extends BuilderBase<T>> {
        @Nullable
        String command = null;
        YTableEntryType<?> inputType = YTableEntryTypes.yson(false);
        YTableEntryType<?> outputType = YTableEntryTypes.yson(false);
        List<YPath> files = new ArrayList<>();
        @Nullable
        DataSize memoryLimit = null;
        boolean useTmpfs = false;
        @Nullable
        DataSize tmpfsSize = null;
        Map<String, String> environment = new HashMap<>();
        @Nullable
        Double cpuLimit = null;
        @Nullable
        Long jobTimeLimit = null;
        @Nullable
        Integer jobCount = null;
        List<YPath> outputTablePaths = new ArrayList<>();

        public CommandSpec build() {
            return new CommandSpec(this);
        }

        /**
         * Set command.
         */
        public T setCommand(String command) {
            this.command = command;
            return self();
        }

        /**
         * Set input type for resolve format of input data.
         */
        public T setInputType(YTableEntryType<?> inputType) {
            this.inputType = inputType;
            return self();
        }

        /**
         * Set output type for resolve format of output data.
         */
        public T setOutputType(YTableEntryType<?> outputType) {
            this.outputType = outputType;
            return self();
        }

        /**
         * Set files which should be available inside job.
         */
        public T setFiles(List<YPath> files) {
            this.files = files;
            return self();
        }

        /**
         * Set memoryLimit which specifies how much memory job process can use.
         */
        public T setMemoryLimit(@Nullable DataSize memoryLimit) {
            this.memoryLimit = memoryLimit;
            return self();
        }

        public T setUseTmpfs(boolean useTmpfs) {
            this.useTmpfs = useTmpfs;
            return self();
        }

        public T setTmpfsSize(@Nullable DataSize tmpfsSize) {
            this.tmpfsSize = tmpfsSize;
            return self();
        }

        /**
         * Set a dictionary of environment variables that will be specified during the operation.
         */
        public T setEnvironment(Map<String, String> environment) {
            this.environment = environment;
            return self();
        }

        /**
         * Set maximum number of CPU cores for a single job to use.
         */
        public T setCpuLimit(@Nullable Double cpuLimit) {
            this.cpuLimit = cpuLimit;
            return self();
        }

        /**
         * Set limit on job execution time.
         * Jobs that exceed this limit will be considered failed.
         */
        public T setJobTimeLimit(@Nullable Long jobTimeLimit) {
            this.jobTimeLimit = jobTimeLimit;
            return self();
        }

        /**
         * Set how many jobs should be run, it is advisory.
         */
        public T setJobCount(@Nullable Integer jobCount) {
            this.jobCount = jobCount;
            return self();
        }

        /**
         * Set output table paths, only for vanilla operations.
         */
        public T setOutputTablePaths(List<YPath> outputTablePaths) {
            this.outputTablePaths = outputTablePaths;
            return self();
        }

        protected abstract T self();
    }
}
