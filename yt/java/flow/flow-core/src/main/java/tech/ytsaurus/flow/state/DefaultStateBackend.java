package tech.ytsaurus.flow.state;

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
    private final Map<String, StatesHolder<InternalState>> internalStates;
    private final Map<String, StatesHolder<ExternalState>> externalStates;
    private final Map<String, StatesHolder<ExternalState>> joinedExternalStates;
    private final @Nullable TableSchema keySchema;

    public DefaultStateBackend(
            Set<String> internalStateNames,
            Set<String> externalStateNames,
            Set<String> joinedExternalStateNames,
            Map<String, StatesHolder<InternalState>> internalStates,
            Map<String, StatesHolder<ExternalState>> externalStates,
            Map<String, StatesHolder<ExternalState>> joinedExternalStates,
            @Nullable TableSchema keySchema
    ) {
        this.internalStateNames = internalStateNames;
        this.externalStateNames = externalStateNames;
        this.joinedExternalStateNames = joinedExternalStateNames;
        this.internalStates = internalStates;
        this.externalStates = externalStates;
        this.joinedExternalStates = joinedExternalStates;
        this.keySchema = keySchema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatesHolder<InternalState> getOrCreateInternalStateHolder(String stateName) {
        validateInternalStateName(stateName);
        return internalStates.computeIfAbsent(stateName, name -> {
            log.debug("Creating new state for name: {}", name);
            return new StatesHolder<>(name, keySchema, null);
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StatesHolder<ExternalState> getExternalStateHolder(String stateName) {
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
    public StatesHolder<ExternalState> getJoinedExternalStateHolder(String stateName) {
        validateJoinedExternalStateName(stateName);
        var holder = joinedExternalStates.get(stateName);
        return holder != null ? holder : new StatesHolder<>(stateName, keySchema, null);
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
}
