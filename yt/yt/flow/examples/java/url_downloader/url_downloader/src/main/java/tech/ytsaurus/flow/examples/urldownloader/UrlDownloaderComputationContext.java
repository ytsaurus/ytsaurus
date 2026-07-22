package tech.ytsaurus.flow.examples.urldownloader;

import java.util.List;

import org.springframework.context.annotation.Configuration;
import tech.ytsaurus.flow.computation.Computation;
import tech.ytsaurus.flow.examples.urldownloader.model.ProcessedUrl;
import tech.ytsaurus.flow.examples.urldownloader.model.UrlMessage;
import tech.ytsaurus.flow.spring.ComputationProvider;
import tech.ytsaurus.flow.stream.FlowStream;
import tech.ytsaurus.flow.stream.FlowStreams;

// [BEGIN computation_context]
@Configuration
public class UrlDownloaderComputationContext implements ComputationProvider {

    @Override
    public List<Computation> getComputations() {
        return List.of(
                Computation.builder()
                        .setComputationId("url_downloader")
                        .setProcessFunction(new UrlDownloadFunction())
                        .build()
        );
    }

    @Override
    public List<FlowStream<?>> getStreams() {
        return List.of(
                FlowStreams.typed("urls", UrlMessage.class),
                FlowStreams.typed("processed_urls", ProcessedUrl.class)
        );
    }
}
// [END computation_context]
