package ru.yandex.yt.ytclient.operations;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.cypress.YPath;
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder;
import ru.yandex.inside.yt.kosher.ytree.YTreeNode;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;
import ru.yandex.yt.ytclient.proxy.TransactionalClient;

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

    public Map<String, YTreeNode> getAdditionalSpecParameters() {
        return additionalSpecParameters;
    }

    @Override
    public YTreeBuilder prepare(YTreeBuilder builder, TransactionalClient yt, SpecPreparationContext context) {
        return builder.beginMap()
                .key("tasks")
                .beginMap()
                .apply(b -> {
                    tasks.forEach((k, v) -> v.prepare(b.key(k), yt, context));
                    return b;
                })
                .endMap()
                .when(maxFailedJobCount != null, b -> b.key("max_failed_job_count").value(maxFailedJobCount))
                .when(stderrTablePath != null, b -> b.key("stderr_table_path").value(stderrTablePath))
                .when(failOnJobRestart != null, b -> b.key("fail_on_job_restart").value(failOnJobRestart))
                .key("started_by").apply(b -> SpecUtils.startedBy(b, context))
                .apply(b -> {
                    for (Map.Entry<String, YTreeNode> node : additionalSpecParameters.entrySet()) {
                        b.key(node.getKey()).value(node.getValue());
                    }
                    return b;
                })
                .endMap();
    }

    public static class Builder extends BuilderBase<Builder> {
        @Override
        protected Builder self() {
            return this;
        }
    }

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

        public T setMaxFailedJobCount(Integer maxFailedJobCount) {
            this.maxFailedJobCount = maxFailedJobCount;
            return self();
        }

        public T setStderrTablePath(YPath stderrTablePath) {
            this.stderrTablePath = stderrTablePath;
            return self();
        }

        public T setFailOnJobRestart(Integer failOnJobRestart) {
            this.failOnJobRestart = failOnJobRestart;
            return self();
        }

        public T setAdditionalSpecParameters(Map<String, YTreeNode> additionalSpecParameters) {
            this.additionalSpecParameters = additionalSpecParameters;
            return self();
        }

        public VanillaSpec build() {
            return new VanillaSpec(this);
        }

        protected abstract T self();
    }
}
