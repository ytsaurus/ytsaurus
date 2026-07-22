package tech.ytsaurus.flow.examples.wordcount;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.examples.wordcount.model.Word;
import tech.ytsaurus.flow.spring.ComputationProvider;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

// [BEGIN stream_context]
@Configuration
public class WordCountContext implements ComputationProvider {

    @Override
    public List<FlowStream<?>> getStreams() {
        return List.of(FlowStreams.typed("words", Word.class));
    }
}
// [END stream_context]
