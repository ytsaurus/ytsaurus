package ru.yandex.yt.ytclient.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTree;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class SystemOperationSpecBase {
    private final List<YPath> inputTables;
    private final YPath outputTable;

    private final @Nullable String pool;
    private final @Nullable String title;

    private final Map<String, YTreeNode> outputTableAttributes;
    private final Map<String, YTreeNode> additionalSpecParameters;

    protected SystemOperationSpecBase(Builder<?> builder) {
        inputTables = new ArrayList<>(builder.inputTables);

        if (builder.outputTable == null) {
            throw new RuntimeException("output table is not set");
        }
        outputTable = builder.outputTable;

        pool = builder.pool;
        title = builder.title;

        outputTableAttributes = builder.outputTableAttributes;
        additionalSpecParameters = builder.additionalSpecParameters;
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
        SystemOperationSpecBase spec = (SystemOperationSpecBase) obj;
        return inputTables.equals(spec.inputTables)
                && outputTable.equals(spec.outputTable)
                && Optional.ofNullable(pool).equals(Optional.ofNullable(spec.pool))
                && Optional.ofNullable(title).equals(Optional.ofNullable(spec.title))
                && outputTableAttributes.equals(spec.outputTableAttributes)
                && additionalSpecParameters.equals(spec.additionalSpecParameters);
    }

    public List<YPath> getInputTables() {
        return inputTables;
    }

    public YPath getOutputTable() {
        return outputTable;
    }

    public Optional<String> getPool() {
        return Optional.ofNullable(pool);
    }

    public Optional<String> getTitle() {
        return Optional.ofNullable(title);
    }

    public Map<String, YTreeNode> getOutputTableAttributes() {
        return outputTableAttributes;
    }

    public Map<String, YTreeNode> getAdditionalSpecParameters() {
        return additionalSpecParameters;
    }

    public YTreeBuilder toTree(YTreeBuilder mapBuilder, SpecPreparationContext context) {
        return mapBuilder
                .key("started_by").apply(b -> SpecUtils.startedBy(b, context))
                .key("input_table_paths").value(inputTables, (b, t) -> t.toTree(b))
                .key("output_table_path").apply(outputTable::toTree)
                .when(pool != null, b -> b.key("pool").value(pool))
                .when(title != null, b -> b.key("title").value(title))
                .apply(b -> {
                    for (Map.Entry<String, YTreeNode> node : additionalSpecParameters.entrySet()) {
                        b.key(node.getKey()).value(node.getValue());
                    }
                    return b;
                });
    }

    @NonNullApi
    @NonNullFields
    public abstract static class Builder<T extends Builder<T>> {
        // N.B. some clients have methods taking this class as argument therefore it must be public
        private List<YPath> inputTables = new ArrayList<>();
        private @Nullable YPath outputTable;

        private @Nullable String pool;
        private @Nullable String title;

        private Map<String, YTreeNode> outputTableAttributes = new HashMap<>();
        private Map<String, YTreeNode> additionalSpecParameters = new HashMap<>();

        public T setInputTables(Collection<YPath> inputTables) {
            this.inputTables = new ArrayList<>(inputTables);
            return self();
        }

        public T setInputTables(YPath... inputTables) {
            return setInputTables(Arrays.asList(inputTables));
        }

        public T addInputTable(YPath inputTable) {
            this.inputTables.add(inputTable);
            return self();
        }

        public T setOutputTable(YPath outputTable) {
            this.outputTable = outputTable;
            return self();
        }

        public T setOutputTableAttributes(Map<String, YTreeNode> outputTableAttributes) {
            this.outputTableAttributes = new HashMap<>(outputTableAttributes);
            return self();
        }

        public T plusOutputTableAttribute(String key, @Nullable Object value) {
            return plusOutputTableAttribute(key, YTree.node(value));
        }

        public T plusOutputTableAttribute(String key, YTreeNode value) {
            this.outputTableAttributes.put(key, value);
            return self();
        }

        public T setPool(@Nullable String pool) {
            this.pool = pool;
            return self();
        }

        public T setTitle(@Nullable String title) {
            this.title = title;
            return self();
        }

        public T setAdditionalSpecParameters(@Nonnull Map<String, YTreeNode> additionalSpecParameters) {
            this.additionalSpecParameters = new HashMap<>(additionalSpecParameters);
            return self();
        }

        public T plusAdditionalSpecParameter(String key, @Nullable Object value) {
            return plusAdditionalSpecParameter(key, YTree.node(value));
        }

        public T plusAdditionalSpecParameter(String key, YTreeNode value) {
            this.additionalSpecParameters.put(key, value);
            return self();
        }

        protected abstract T self();
    }
}
