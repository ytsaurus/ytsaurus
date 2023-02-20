package tech.ytsaurus.client.operations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.TransactionalClient;
import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * Immutable vanilla spec.
 *
 * @see <a href="https://yt.yandex-team.ru/docs/description/mr/vanilla">
 * vanilla documentation
 * </a>
 */
@NonNullApi
@NonNullFields
public class VanillaSpec implements Spec {
    private final Map<String, ? extends Spec> tasks;
    @Nullable
    private final Integer maxFailedJobCount;
    @Nullable
    private final YPath stderrTablePath;
    @Nullable
    private final Integer failOnJobRestart;
    private final Map<String, YTreeNode> additionalSpecParameters;

    protected <T extends BuilderBase<T>> VanillaSpec(BuilderBase<T> builder) {
        if (builder.tasks == null) {
            throw new IllegalArgumentException("Not null tasks was expected");
        }
        tasks = builder.tasks;
        maxFailedJobCount = builder.maxFailedJobCount;
        stderrTablePath = builder.stderrTablePath;
        failOnJobRestart = builder.failOnJobRestart;
        additionalSpecParameters = builder.additionalSpecParameters;
    }

    public <TSpec extends Spec> VanillaSpec(String taskName, TSpec spec) {
        this(builder().setTask(taskName, spec));
    }

    public static BuilderBase<?> builder() {
        return new Builder();
    }

    public Map<String, ? extends Spec> getTasks() {
        return tasks;
    }

    public Optional<Integer> getMaxFailedJobCount() {
        return Optional.ofNullable(maxFailedJobCount);
    }

    public Optional<YPath> getStderrTablePath() {
        return Optional.ofNullable(stderrTablePath);
    }

    public Optional<Integer> getFailOnJobRestart() {
        return Optional.ofNullable(failOnJobRestart);
    }

    /**
     * @see Builder#setAdditionalSpecParameters(Map)
     */
    public Map<String, YTreeNode> getAdditionalSpecParameters() {
        return additionalSpecParameters;
    }

    /**
     * Convert to yson.
     */
    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt,
                                SpecPreparationContext specPreparationContext) {
        return builder.beginMap()
                .key("tasks")
                .beginMap()
                .apply(b -> {
                    tasks.forEach((k, v) -> v.prepare(b.key(k), yt, specPreparationContext));
                    return b;
                })
                .endMap()
                .when(maxFailedJobCount != null, b -> b.key("max_failed_job_count").value(maxFailedJobCount))
                .when(stderrTablePath != null, b -> b.key("stderr_table_path").value(stderrTablePath))
                .when(failOnJobRestart != null, b -> b.key("fail_on_job_restart").value(failOnJobRestart))
                .key("started_by").apply(b -> SpecUtils.startedBy(b, specPreparationContext))
                .apply(b -> {
                    for (Map.Entry<String, YTreeNode> node : additionalSpecParameters.entrySet()) {
                        b.key(node.getKey()).value(node.getValue());
                    }
                    return b;
                })
                .endMap();
    }

    /**
     * Builder of {@link VanillaSpec}.
     */
    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

    // BuilderBase was taken out because there is another client
    // which we need to support too and which use the same VanillaSpec class.
    @NonNullApi
    @NonNullFields
    public abstract static class BuilderBase<T extends BuilderBase<T>> {
        @Nullable
        private Map<String, ? extends Spec> tasks;
        @Nullable
        private Integer maxFailedJobCount;
        @Nullable
        private YPath stderrTablePath;
        @Nullable
        private Integer failOnJobRestart;
        private Map<String, YTreeNode> additionalSpecParameters = new HashMap<>();

        public T setTasks(Map<String, ? extends Spec> tasks) {
            this.tasks = tasks;
            return self();
        }

        public <TSpec extends Spec> T setTask(String taskName, TSpec spec) {
            this.tasks = Collections.singletonMap(taskName, spec);
            return self();
        }

        /**
         * Set the number of failed jobs after which the operation is considered failed.
         * The default is taken from the cluster settings, where this value is usually 10;
         */
        public T setMaxFailedJobCount(Integer maxFailedJobCount) {
            this.maxFailedJobCount = maxFailedJobCount;
            return self();
        }

        /**
         * Set a path to an existing table can be specified, the table must be created outside of transactions.
         * If this setting is specified, then the full data from the standard error stream (stderr) of all jobs (except
         * aborted) will be written to the specified table. The table will have the following columns:
         * job_id — job ID;
         * part_index - in case of a large error message, the stderr message of one job can be divided into several
         * parts (in different lines), this column contains the number of such a part;
         * data - data from the standard error stream.
         */
        public T setStderrTablePath(YPath stderrTablePath) {
            this.stderrTablePath = stderrTablePath;
            return self();
        }

        /**
         * If true, at any completion of the job, which had started its work, other than successful
         * (that is, with abort or fail), forcibly abort the operation.
         * Useful for operations where restarting jobs is not allowed.
         */
        public T setFailOnJobRestart(Integer failOnJobRestart) {
            this.failOnJobRestart = failOnJobRestart;
            return self();
        }

        /**
         * Set additional spec parameters.
         */
        public T setAdditionalSpecParameters(Map<String, YTreeNode> additionalSpecParameters) {
            this.additionalSpecParameters = additionalSpecParameters;
            return self();
        }

        /**
         * Create instance of {@link VanillaSpec}.
         */
        public VanillaSpec build() {
            return new VanillaSpec(this);
        }

        protected abstract T self();
    }
}
