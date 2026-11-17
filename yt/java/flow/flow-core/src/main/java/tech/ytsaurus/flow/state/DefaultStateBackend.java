package tech.ytsaurus.flow.state;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.core.tables.TableSchema;

/**
 * Default implementation of {@link StateBackend}.
 */
public class DefaultStateBackend implements StateBackend {
    private static final Logger log = LoggerFactory.getLogger(DefaultStateBackend.class);

    private final Set<String> internalStateNames;
    private final Set<String> externalStateNames;
    private final Set<String> joinedExternalStateNames;
    private final Map<String, StatesHolder> internalStates;
    private final Map<String, StatesHolder> externalStates;
    private final Map<String, StatesHolder> joinedExternalStates;
    private final @Nullable TableSchema keySchema;

    private DefaultStateBackend(Builder builder) {
        this.internalStateNames = builder.internalStateNames;
        this.externalStateNames = builder.externalStateNames;
        this.joinedExternalStateNames = builder.joinedExternalStateNames;
        this.internalStates = builder.internalStates;
        this.externalStates = builder.externalStates;
        this.joinedExternalStates = builder.joinedExternalStates;
        this.keySchema = builder.keySchema;
    }

    /**
     * Creates a builder. Every state kind defaults to no declared states and no holders.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatesHolder getOrCreateInternalStateHolder(String stateName) {
        validateInternalStateName(stateName);
        return internalStates.computeIfAbsent(stateName, name -> {
            log.debug("Creating new state for name: {}", name);
            return new StatesHolder(name, keySchema, null);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatesHolder getExternalStateHolder(String stateName) {
        validateExternalStateName(stateName);
        return Objects.requireNonNull(
                externalStates.get(stateName),
                "External state %s not found".formatted(stateName)
        );
    }

    /**
     * {@inheritDoc}
     *
     * <p>Returns an empty (schema-less) holder when no rows were joined for the requested keys:
     * the joiner is declared but the upstream state table had no matching entries.
     */
    @Override
    public StatesHolder getJoinedExternalStateHolder(String stateName) {
        validateJoinedExternalStateName(stateName);
        var holder = joinedExternalStates.get(stateName);
        return holder != null ? holder : new StatesHolder(stateName, keySchema, null);
    }

    private void validateInternalStateName(String stateName) {
        if (!internalStateNames.contains(stateName)) {
            throw new IllegalArgumentException(
                    "State must be configured at computation static spec parameters (StateName: %s)"
                            .formatted(stateName)
            );
        }
    }

    /**
     * Validates that the external state is declared in the computation static spec under
     * {@code external_state_managers}. The set of declared names is provided by the caller
     * (typically derived from {@code Job.getExternalStatesNames()}).
     */
    private void validateExternalStateName(String stateName) {
        if (!externalStateNames.contains(stateName)) {
            throw new IllegalArgumentException(
                    "External state must be configured at computation static spec under "
                            + "external_state_managers (StateName: %s)".formatted(stateName)
            );
        }
    }

    private void validateJoinedExternalStateName(String stateName) {
        if (!joinedExternalStateNames.contains(stateName)) {
            throw new IllegalArgumentException(
                    "Joined external state must be configured at computation static spec under "
                            + "external_state_joiners (StateName: %s)".formatted(stateName)
            );
        }
    }

    /**
     * Builder for {@link DefaultStateBackend}.
     */
    public static class Builder {
        private Set<String> internalStateNames = Collections.emptySet();
        private Set<String> externalStateNames = Collections.emptySet();
        private Set<String> joinedExternalStateNames = Collections.emptySet();
        private Map<String, StatesHolder> internalStates = new HashMap<>();
        private Map<String, StatesHolder> externalStates = Collections.emptyMap();
        private Map<String, StatesHolder> joinedExternalStates = Collections.emptyMap();
        private @Nullable TableSchema keySchema;

        Builder() {
        }

        /**
         * Sets the internal states declared in the computation static spec.
         */
        public Builder setInternalStateNames(Set<String> internalStateNames) {
            this.internalStateNames = internalStateNames;
            return this;
        }

        /**
         * Sets the external states declared under {@code external_state_managers}.
         */
        public Builder setExternalStateNames(Set<String> externalStateNames) {
            this.externalStateNames = externalStateNames;
            return this;
        }

        /**
         * Sets the joined external states declared under {@code external_state_joiners}.
         */
        public Builder setJoinedExternalStateNames(Set<String> joinedExternalStateNames) {
            this.joinedExternalStateNames = joinedExternalStateNames;
            return this;
        }

        /**
         * Sets the internal state holders keyed by state name.
         *
         * <p>The map must be mutable: the backend adds a holder for every internal state first
         * accessed in the batch, and the caller reads them back from the same map.
         */
        public Builder setInternalStates(Map<String, StatesHolder> internalStates) {
            this.internalStates = internalStates;
            return this;
        }

        /**
         * Sets the external state holders keyed by state name.
         */
        public Builder setExternalStates(Map<String, StatesHolder> externalStates) {
            this.externalStates = externalStates;
            return this;
        }

        /**
         * Sets the read-only joined external state holders keyed by state name.
         */
        public Builder setJoinedExternalStates(Map<String, StatesHolder> joinedExternalStates) {
            this.joinedExternalStates = joinedExternalStates;
            return this;
        }

        /**
         * Sets the key schema of the holders the backend creates.
         */
        public Builder setKeySchema(@Nullable TableSchema keySchema) {
            this.keySchema = keySchema;
            return this;
        }

        public DefaultStateBackend build() {
            return new DefaultStateBackend(this);
        }
    }
}
