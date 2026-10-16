package tech.ytsaurus.flow.examples.urldownloader;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.examples.urldownloader.model.ProcessedUrl;
import tech.ytsaurus.flow.examples.urldownloader.model.UrlMessage;
import tech.ytsaurus.flow.spring.ComputationProvider;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

// [BEGIN stream_context]
@Configuration
public class UrlDownloaderComputationContext implements ComputationProvider {

    @Override
    public List<FlowStream<?>> getStreams() {
        return List.of(
                FlowStreams.typed("urls", UrlMessage.class),
                FlowStreams.typed("processed_urls", ProcessedUrl.class)
        );
    }
}
// [END stream_context]
