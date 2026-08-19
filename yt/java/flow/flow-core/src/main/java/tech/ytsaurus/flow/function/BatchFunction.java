package tech.ytsaurus.flow.function;

import java.util.List;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.Visit;

/**
 * An interface for per-batch functions.
 */
public non-sealed interface BatchFunction extends ProcessFunction<Payload> {

    /**
     * Method called for each input batch of messages.
     * The implementation might write any numbers of messages to the output collector.
     *
     * @param messages List of input messages (batch).
     * @param output   Output collector for result.
     * @param ctx      Runtime context.
     */
    void onMessages(List<ExtendedMessage> messages, OutputCollector output, RuntimeContext ctx);

    /**
     * Method called for each input batch of timers.
     * The implementation might write any numbers of messages to the output collector.
     *
     * @param timers List of input timers.
     * @param output Output collector for result.
     * @param ctx    Runtime context.
     */
    default void onTimers(List<Timer> timers, OutputCollector output, RuntimeContext ctx) {
    }

    /**
     * Method called for each input batch of visits.
     * The implementation might write any numbers of messages to the output collector.
     *
     * @param visits List of input visits.
     * @param output Output collector for result.
     * @param ctx    Runtime context.
     */
    default void onVisits(List<Visit> visits, OutputCollector output, RuntimeContext ctx) {
    }

}
