package ru.yandex.yt.ytclient.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;

import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullApi
@NonNullFields
public class UserOperationSpecBase {
    private final List<YPath> inputTables;
    private final List<YPath> outputTables;

    private final @Nullable String pool;
    private final @Nullable String title;

    private final Map<String, String> secureVault;
    private final Map<String, YTreeNode> additionalSpecParameters;
    private final Map<String, YTreeNode> outputTableAttributes;

    protected UserOperationSpecBase(Builder<?> builder) {
        if (builder.inputTables.isEmpty()) {
            throw new RuntimeException("input tables are not set");
        }
        if (builder.outputTables.isEmpty()) {
            throw new RuntimeException("output tables are not set");
        }
        inputTables = builder.inputTables;
        outputTables = builder.outputTables;

        pool = builder.pool;
        title = builder.title;

        secureVault = new HashMap<>(builder.secureVault);
        additionalSpecParameters = new HashMap<>(builder.additionalSpecParameters);
        outputTableAttributes = new HashMap<>(builder.outputTableAttributes);
    }

    public List<YPath> getInputTables() {
        return inputTables;
    }

    public List<YPath> getOutputTables() {
        return outputTables;
    }

    public Optional<String> getPool() {
        return Optional.ofNullable(pool);
    }

    public Optional<String> getTitle() {
        return Optional.ofNullable(title);
    }

    public Map<String, String> getSecureVault() {
        return secureVault;
    }

    public Map<String, YTreeNode> getOutputTableAttributes() {
        return outputTableAttributes;
    }

    public Map<String, YTreeNode> getAdditionalSpecParameters() {
        return additionalSpecParameters;
    }

    protected YTreeBuilder toTree(YTreeBuilder mapBuilder, SpecPreparationContext context) {
        mapBuilder
                .key("started_by").apply(b -> SpecUtils.startedBy(b, context))
                .key("input_table_paths").value(inputTables, (b, t) -> t.toTree(b))
                .key("output_table_paths").value(outputTables, (b, t) -> t.toTree(b))
                .when(pool != null, b -> b.key("pool").value(pool))
                .when(title != null, b -> b.key("title").value(title))
                .when(!secureVault.isEmpty(), b -> b.key("secure_vault").value(secureVault))
                .apply(b -> {
                    for (Map.Entry<String, YTreeNode> node : additionalSpecParameters.entrySet()) {
                        b.key(node.getKey()).value(node.getValue());
                    }
                    return b;
                });
        return mapBuilder;
    }

    @NonNullApi
    @NonNullFields
    public abstract static class Builder<T extends UserOperationSpecBase.Builder<T>> {
        // N.B. some clients have methods taking this class as argument therefore it must be public
        private List<YPath> inputTables = new ArrayList<>();
        private List<YPath> outputTables = new ArrayList<>();

        private @Nullable String pool;
        private @Nullable String title;

        private Map<String, String> secureVault = new HashMap<>();
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

        public T setOutputTables(Collection<YPath> outputTables) {
            this.outputTables = new ArrayList<>(outputTables);
            return self();
        }

        public T setOutputTables(YPath... inputTables) {
            return setOutputTables(Arrays.asList(inputTables));
        }

        public T addOutputTable(YPath outputTable) {
            this.outputTables.add(outputTable);
            return self();
        }

        public T setOutputTableAttributes(Map<String, YTreeNode> outputTableAttributes) {
            this.outputTableAttributes = new HashMap<>(outputTableAttributes);
            return self();
        }

        public T plusOutputTableAttribute(String key, @Nullable Object value) {
            YTreeNode node = new YTreeBuilder().value(value).build();
            return plusOutputTableAttribute(key, node);
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

        public T setSecureVault(Map<String, String> secureVault) {
            this.secureVault = new HashMap<>(secureVault);
            return self();
        }

        public T plusSecureVault(String key, String value) {
            this.secureVault.put(key, value);
            return self();
        }

        public T setAdditionalSpecParameters(Map<String, YTreeNode> additionalSpecParameters) {
            this.additionalSpecParameters = new HashMap<>(additionalSpecParameters);
            return self();
        }

        public T plusAdditionalSpecParameter(String key, @Nullable Object value) {
            YTreeNode node = new YTreeBuilder().value(value).build();
            return plusAdditionalSpecParameter(key, node);
        }

        public T plusAdditionalSpecParameter(String key, YTreeNode value) {
            this.additionalSpecParameters.put(key, value);
            return self();
        }

        protected abstract T self();
    }
}
