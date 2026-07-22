package tech.ytsaurus.flow.computation;

import java.util.List;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.row.Message;

/**
 * Interface for collecting output messages with parent IDs.
 */
public interface OutputCollector {

    /**
     * Creates a new {@link OutputCollector} instance with provided parent messages IDs.
     *
     * @param parentIds List of message IDs to track as parents.
     * @return A new {@link OutputCollector} instance that will associate added messages with these parent.
     */
    OutputCollector setParentIds(List<String> parentIds);

    /**
     * Creates a new {@link OutputCollector} instance with provided parent messages IDs.
     *
     * @param parentIds Array of message IDs to track as parents.
     * @return A new {@link OutputCollector} instance that will associate added messages with these parent.
     */
    default OutputCollector setParentIds(String... parentIds) {
        return setParentIds(List.of(parentIds));
    }

    /**
     * Adds a message to the output collector.
     *
     * @param message The message to be added
     */
    default void addMessage(Message message) {
        addMessage(message, true);
    }

    /**
     * Adds a message to the output collector.
     *
     * @param message    The message to be added
     * @param distribute If false, the message is not published downstream but is still counted
     *                   by the watermark generator. Used by source computations.
     */
    void addMessage(Message message, boolean distribute);

    /**
     * Adds a timer to the output collector.
     * This method is supposed to be used in case of single timer's stream.
     * StreamId would be guessed from the spec.
     * <p>
     * EventTimestamp would be set at worker side.
     * <p>
     * In case of multiple timers' streams, use {@link #addTimer(String, long, long)} instead.
     *
     * @param triggerTimestamp The timestamp when the timer should be triggered.
     */
    default void addTimer(long triggerTimestamp) {
        addTimer(triggerTimestamp, 0);
    }

    /**
     * Adds a timer to the output collector.
     * This method is supposed to be used in case of single timer's stream.
     * StreamId would be guessed from the spec.
     * <p>
     * In case of multiple timers' streams, use {@link #addTimer(String, long, long)} instead.
     *
     * @param triggerTimestamp The timestamp when the timer should be triggered.
     * @param eventTimestamp   The timestamp of the event. Use 0 if you don't want to specify it.
     */
    default void addTimer(long triggerTimestamp, long eventTimestamp) {
        addTimer(null, triggerTimestamp, eventTimestamp);
    }

    /**
     * Adds a timer to the output collector.
     *
     * @param timerStreamId    The stream ID of the timer; null selects the single timer stream from the spec.
     * @param triggerTimestamp The timestamp when the timer should be triggered.
     * @param eventTimestamp   The timestamp of the event. Use 0 if you don't want to specify it.
     */
    void addTimer(@Nullable String timerStreamId, long triggerTimestamp, long eventTimestamp);
}
