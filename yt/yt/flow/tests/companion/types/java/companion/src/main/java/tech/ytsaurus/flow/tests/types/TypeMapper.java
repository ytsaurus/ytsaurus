package tech.ytsaurus.flow.tests.types;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;

/**
 * Performs a straightforward identity mapping of every typed field: reads each
 * column of the incoming message and writes the same value to the output
 * stream, exercising the companion wire-protocol round-trip for every distinct
 * value type (string, int64, uint64, double, boolean).
 */
public class TypeMapper implements RowFunction {

    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        output.addMessage(ctx.createMessageBuilder("mapped")
                .set("key", message.get("key", String.class))
                .set("f_string", message.get("f_string", String.class))
                .set("f_int64", message.get("f_int64", Long.class))
                .set("f_uint64", message.get("f_uint64", Long.class))
                .set("f_double", message.get("f_double", Double.class))
                .set("f_bool", message.get("f_bool", Boolean.class))
                .finish());
    }
}
