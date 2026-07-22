package tech.ytsaurus.flow.examples.wordcount;

import org.springframework.stereotype.Component;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.examples.wordcount.model.Word;
import tech.ytsaurus.flow.examples.wordcount.model.WordCountState;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.state.InternalStateDescriptor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

@Component
public class WordCountMapper implements RowFunction {
    static final InternalStateDescriptor<WordCountState> WORD_STATE =
            StateDescriptors.yson("word-state", WordCountState.class);

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        Word input = message.getPayload();
        StateAccessor<WordCountState> stateAccessor = ctx.getState(WORD_STATE, message);
        var state = stateAccessor.getOrDefault(new WordCountState(input.getWord(), 0));
        state.setCount(state.getCount() + 1);
        stateAccessor.set(state);
    }
    // [END on_message]
}
