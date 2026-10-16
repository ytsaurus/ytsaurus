package tech.ytsaurus.flow.function;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Payload;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.row.Visit;

/**
 * An interface for per-message functions.
 */
public non-sealed interface RowFunction extends ProcessFunction<Payload> {

    /**
     * Method called for each input message.
     * The implementation might write any numbers of messages to the output collector.
     *
     * @param message Input message.
     * @param output  Output collector for result.
     * @param ctx     Runtime context.
     */
    void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx);

    /**
     * Method called for each triggered timer.
     * The implementation might write any numbers of messages to the output collector.
     *
     * @param timer  Timer.
     * @param output Output collector for result.
     * @param ctx    Runtime context.
     */
    default void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
    }

    /**
     * Method called for each visit produced by a key-visitor stream.
     * The implementation might write any numbers of messages to the output collector.
     *
     * @param visit  Visit.
     * @param output Output collector for result.
     * @param ctx    Runtime context.
     */
    default void onVisit(Visit visit, OutputCollector output, RuntimeContext ctx) {
    }
}
