package tech.ytsaurus.flow.examples.urldownloader

import org.springframework.context.annotation.Configuration
import tech.ytsaurus.flow.examples.urldownloader.model.ProcessedUrl
import tech.ytsaurus.flow.examples.urldownloader.model.UrlMessage
import tech.ytsaurus.flow.spring.ComputationProvider
import tech.ytsaurus.flow.stream.FlowStream
import tech.ytsaurus.flow.stream.FlowStreams

// [BEGIN stream_context]
@Configuration
open class UrlDownloaderComputationContext : ComputationProvider {

    override fun getStreams(): List<FlowStream<*>> {
        return listOf(
            FlowStreams.typed("urls", UrlMessage::class.java),
            FlowStreams.typed("processed_urls", ProcessedUrl::class.java),
        )
    }
}
// [END stream_context]
