package tech.ytsaurus.flow.testutils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import tech.ytsaurus.flow.computation.TransformResult;
import tech.ytsaurus.flow.request.ResponseContext;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.NewTimer;
import tech.ytsaurus.flow.state.StatesHolder;

/**
 * The response from {@code TestComputationHarness.doProcess()}, which contains all outputs
 * of a computation execution.
 * <p>Intended to be used for verification in unit tests.</p>
 */
public class TestDoProcessResponse {
    private final ResponseContext responseContext;
    private final StateView allStates;
    private final StateView modifiedStates;

    /**
     * Creates a response that merges the states the request carried with the computation's
     * changes. Both sides are parsed off the wire, so the views agree with what the computation
     * itself observed.
     *
     * @param responseContext       the processing result (carries the modified states)
     * @param requestExternalStates external states as parsed from the request, by name
     * @param requestInternalStates internal states as parsed from the request, by name
     */
    TestDoProcessResponse(
            ResponseContext responseContext,
            Map<String, StatesHolder> requestExternalStates,
            Map<String, StatesHolder> requestInternalStates
    ) {
        this.responseContext = responseContext;
        var views = StateViews.from(
                responseContext.getExternalStates(), responseContext.getInternalStates(),
                requestExternalStates, requestInternalStates);
        this.allStates = views.all();
        this.modifiedStates = views.modified();
    }

    /**
     * Returns the deserialized transform results from the response.
     * Each {@link TransformResult} contains output messages, timers, and parent IDs.
     */
    public List<TransformResult> getTransformResults() {
        return responseContext.getTransformResults();
    }

    /**
     * Returns all output messages from all transform result groups, flattened into a single list.
     */
    public List<Message> getOutputMessagesFlatten() {
        return responseContext.getTransformResults().stream()
                .flatMap(tr -> tr.getMessages().stream())
                .toList();
    }

    /**
     * Returns all new timers from all transform result groups, flattened into a single list.
     */
    public List<NewTimer> getOutputTimersFlatten() {
        return responseContext.getTransformResults().stream()
                .flatMap(tr -> tr.getTimers().stream())
                .toList();
    }

    /**
     * Returns the read-only view over all states: request states with the computation's changes
     * overlaid. Every external state the request carried is named here, including those with no
     * entries.
     */
    public StateView allStates() {
        return allStates;
    }

    /**
     * Returns the read-only view over only the states the computation modified.
     */
    public StateView modifiedStates() {
        return modifiedStates;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestDoProcessResponse that = (TestDoProcessResponse) o;
        return Objects.equals(responseContext, that.responseContext);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(responseContext);
    }
}
