package tech.ytsaurus.flow.examples.noop;

import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.spring.FlowSourceComputation;

// Receives the records of the source and emits nothing; put your logic here.
@FlowSourceComputation(id = "reader")
public class Reader implements RowFunction {
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
    }
}
