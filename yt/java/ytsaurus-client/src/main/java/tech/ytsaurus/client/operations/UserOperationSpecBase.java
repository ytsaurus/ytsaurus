package tech.ytsaurus.client.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.core.cypress.YPath;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;
import tech.ytsaurus.ysontree.YTreeBuilder;
import tech.ytsaurus.ysontree.YTreeNode;


/**
 * Immutable base class for some specs.
 */
@NonNullApi
@NonNullFields
public class UserOperationSpecBase {
    private final List<YPath> inputTables;
    private final List<YPath> outputTables;

    @Nullable
    private final String pool;
    @Nullable
    private final String title;

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

        @Nullable
        private String pool;
        @Nullable
        private String title;

        private Map<String, String> secureVault = new HashMap<>();
        private Map<String, YTreeNode> outputTableAttributes = new HashMap<>();
        private Map<String, YTreeNode> additionalSpecParameters = new HashMap<>();

        /**
         * Set input tables.
         */
        public T setInputTables(Collection<YPath> inputTables) {
            this.inputTables = new ArrayList<>(inputTables);
            return self();
        }

        /**
         * Set input tables.
         */
        public T setInputTables(YPath... inputTables) {
            return setInputTables(Arrays.asList(inputTables));
        }

        /**
         * Add one more input table.
         */
        public T addInputTable(YPath inputTable) {
            this.inputTables.add(inputTable);
            return self();
        }

        /**
         * Set output tables.
         */
        public T setOutputTables(Collection<YPath> outputTables) {
            this.outputTables = new ArrayList<>(outputTables);
            return self();
        }

        /**
         * Set output tables.
         */
        public T setOutputTables(YPath... inputTables) {
            return setOutputTables(Arrays.asList(inputTables));
        }

        /**
         * Add one more output table.
         */
        public T addOutputTable(YPath outputTable) {
            this.outputTables.add(outputTable);
            return self();
        }

        /**
         * Set attributes of output table.
         */
        public T setOutputTableAttributes(Map<String, YTreeNode> outputTableAttributes) {
            this.outputTableAttributes = new HashMap<>(outputTableAttributes);
            return self();
        }

        /**
         * Add one more attribute of output table.
         */
        public T plusOutputTableAttribute(String key, @Nullable Object value) {
            YTreeNode node = new YTreeBuilder().value(value).build();
            return plusOutputTableAttribute(key, node);
        }

        /**
         * Add one more attribute of output table.
         */
        public T plusOutputTableAttribute(String key, YTreeNode value) {
            this.outputTableAttributes.put(key, value);
            return self();
        }

        /**
         * Set pool where operation should be run.
         */
        public T setPool(@Nullable String pool) {
            this.pool = pool;
            return self();
        }

        /**
         * Set title of operation.
         */
        public T setTitle(@Nullable String title) {
            this.title = title;
            return self();
        }

        /**
         * Values from this dictionary will be included in the environments of all custom jobs of this operation,
         * and will not be available for viewing by outside users
         * (unlike the environment section in the custom job specification).
         * Namely, the entire passed dictionary in YSON format will be written to the YT_SECURE_VAULT
         * environment variable, and for ease of use, for each key=value pair from the dictionary,
         * the value value will be written to the YT_SECURE_VAULT_key environment variable
         * (this will happen only if value is a primitive type, i.e. int64 , uint64, double, boolean, or string);
         */
        public T setSecureVault(Map<String, String> secureVault) {
            this.secureVault = new HashMap<>(secureVault);
            return self();
        }

        /**
         * @see Builder#setSecureVault(Map)
         */
        public T plusSecureVault(String key, String value) {
            this.secureVault.put(key, value);
            return self();
        }

        /**
         * Set additional parameters of spec.
         * For setting parameters which are not supported explicitly.
         */
        public T setAdditionalSpecParameters(Map<String, YTreeNode> additionalSpecParameters) {
            this.additionalSpecParameters = new HashMap<>(additionalSpecParameters);
            return self();
        }

        /**
         * Add additional parameter of spec.
         * For setting parameters which are not supported explicitly.
         */
        public T plusAdditionalSpecParameter(String key, @Nullable Object value) {
            YTreeNode node = new YTreeBuilder().value(value).build();
            return plusAdditionalSpecParameter(key, node);
        }

        /**
         * Add additional parameter of spec.
         * For setting parameters which are not supported explicitly.
         */
        public T plusAdditionalSpecParameter(String key, YTreeNode value) {
            this.additionalSpecParameters.put(key, value);
            return self();
        }

        protected abstract T self();
    }
}
