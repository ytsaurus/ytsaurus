package ru.yandex.yt.ytclient.operations;

import java.util.Collections;
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

    public <TSpec extends Spec> VanillaSpec(String taskName, TSpec spec) {
        this(Collections.singletonMap(taskName, spec));
    }

    public VanillaSpec(Map<String, ? extends Spec> tasks) {
        this(tasks, null, null, null, Collections.emptyMap());
    }

    public VanillaSpec(
            Map<String, ? extends Spec> tasks,
            @Nullable Integer maxFailedJobCount,
            @Nullable Integer failOnJobRestart,
            @Nullable YPath stderrTablePath,
            Map<String, YTreeNode> additionalSpecParameters) {
        this.tasks = tasks;
        this.maxFailedJobCount = maxFailedJobCount;
        this.failOnJobRestart = failOnJobRestart;
        this.stderrTablePath = stderrTablePath;
        this.additionalSpecParameters = additionalSpecParameters;
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
}
