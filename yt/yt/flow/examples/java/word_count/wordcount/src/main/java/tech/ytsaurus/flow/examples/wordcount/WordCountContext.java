package tech.ytsaurus.flow.examples.wordcount;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.examples.wordcount.model.Word;
import tech.ytsaurus.flow.spring.ComputationProvider;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

// [BEGIN word_count_context]
@Configuration
public class WordCountContext implements ComputationProvider {

    @Autowired
    private WordCountMapper wordCountMapper;

    @Override
    public List<Computation> getComputations() {
        // "reader" is a native passthrough source (declared in the pipeline spec via
        // computation_class_name) and is therefore not registered with the companion.
        Computation mapper = Computation.builder()
                .setComputationId("mapper")
                .setProcessFunction(wordCountMapper)
                .build();
        return List.of(mapper);
    }

    @Override
    public List<FlowStream<?>> getStreams() {
        return List.of(FlowStreams.typed("words", Word.class));
    }
}
// [END word_count_context]
