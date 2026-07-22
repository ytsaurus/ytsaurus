package tech.ytsaurus.flow.examples.urldownloader

import org.slf4j.LoggerFactory
import tech.ytsaurus.flow.computation.OutputCollector
import tech.ytsaurus.flow.context.RuntimeContext
import tech.ytsaurus.flow.examples.urldownloader.model.HostState
import tech.ytsaurus.flow.examples.urldownloader.model.ProcessedUrl
import tech.ytsaurus.flow.examples.urldownloader.model.UrlMessage
import tech.ytsaurus.flow.function.RowFunction
import tech.ytsaurus.flow.row.ExtendedMessage
import tech.ytsaurus.flow.row.Message
import tech.ytsaurus.flow.row.Timer
import tech.ytsaurus.flow.state.InternalStateDescriptor
import tech.ytsaurus.flow.state.StateDescriptors

class UrlDownloadFunction : RowFunction {
    companion object {
        private val log = LoggerFactory.getLogger(UrlDownloadFunction::class.java)

        private val HOST_STATE: InternalStateDescriptor<HostState> =
            StateDescriptors.yson("host-state", HostState::class.java)
    }

    // [BEGIN on_message]
    override fun onMessage(message: ExtendedMessage, output: OutputCollector, ctx: RuntimeContext) {
        val urlMessage: UrlMessage = message.getPayload()
        val host = urlMessage.host
        val url = urlMessage.url

        val accessor = ctx.getState(HOST_STATE, message)
        val state = accessor.getOrDefault(HostState(host))
        state.pendingUrls!!.add(url)
        accessor.set(state)

        output.addTimer(System.currentTimeMillis() / 1000 + 5, 0L)
        log.debug("Queued url (Host: {}, Url: {})", host, url)
    }
    // [END on_message]

    // [BEGIN on_timer]
    override fun onTimer(timer: Timer, output: OutputCollector, ctx: RuntimeContext) {
        val accessor = ctx.getState(HOST_STATE, timer)
        val stateOpt = accessor.get()
        if (stateOpt.isEmpty || stateOpt.get().pendingUrls.isNullOrEmpty()) {
            accessor.clear()
            return
        }
        val state = stateOpt.get()
        val host = state.host

        for (url in ArrayList(state.pendingUrls)) {
            val data = processUrl(url!!)
            val processedUrl = ProcessedUrl(host, url, data)
            output.addMessage(Message("processed_urls", processedUrl))
            log.debug("Processed url (Host: {}, Url: {})", host, url)
        }
        accessor.clear()
    }
    // [END on_timer]

    private fun processUrl(url: String): String {
        val digits = url.chars().filter(Character::isDigit).count()
        return "length: " + url.length + ", digits: " + digits
    }
}
