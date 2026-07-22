package tech.ytsaurus.flow.examples.urldownloader

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.computation.Computation
import tech.ytsaurus.flow.examples.urldownloader.model.ProcessedUrl
import tech.ytsaurus.flow.examples.urldownloader.model.UrlMessage
import tech.ytsaurus.flow.spring.ComputationProvider
import tech.ytsaurus.flow.stream.FlowStream
import tech.ytsaurus.flow.stream.FlowStreams

// [BEGIN computation_context]
@Configuration
open class UrlDownloaderComputationContext : ComputationProvider {

    override fun getComputations(): List<Computation> {
        return listOf(
            Computation.builder()
                .setComputationId("url_downloader")
                .setProcessFunction(UrlDownloadFunction())
                .build(),
        )
    }

    override fun getStreams(): List<FlowStream<*>> {
        return listOf(
            FlowStreams.typed("urls", UrlMessage::class.java),
            FlowStreams.typed("processed_urls", ProcessedUrl::class.java),
        )
    }
}
// [END computation_context]
