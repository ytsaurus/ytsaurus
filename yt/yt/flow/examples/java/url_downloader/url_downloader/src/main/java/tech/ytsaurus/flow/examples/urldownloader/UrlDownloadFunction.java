package tech.ytsaurus.flow.examples.urldownloader;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.context.RuntimeContext;
import tech.ytsaurus.flow.examples.urldownloader.model.HostState;
import tech.ytsaurus.flow.examples.urldownloader.model.ProcessedUrl;
import tech.ytsaurus.flow.examples.urldownloader.model.UrlMessage;
import tech.ytsaurus.flow.function.RowFunction;
import tech.ytsaurus.flow.row.ExtendedMessage;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.Timer;
import tech.ytsaurus.flow.state.InternalStateDescriptor;
import tech.ytsaurus.flow.state.StateAccessor;
import tech.ytsaurus.flow.state.StateDescriptors;

public class UrlDownloadFunction implements RowFunction {
    private static final Logger log = LoggerFactory.getLogger(UrlDownloadFunction.class);

    private static final InternalStateDescriptor<HostState> HOST_STATE =
            StateDescriptors.yson("host-state", HostState.class);

    // [BEGIN on_message]
    @Override
    public void onMessage(ExtendedMessage message, OutputCollector output, RuntimeContext ctx) {
        UrlMessage urlMessage = message.getPayload();
        String host = urlMessage.getHost();
        String url = urlMessage.getUrl();

        StateAccessor<HostState> accessor = ctx.getState(HOST_STATE, message);
        HostState state = accessor.getOrDefault(new HostState(host));
        state.getPendingUrls().add(url);
        accessor.set(state);

        output.addTimer(System.currentTimeMillis() / 1000 + 5, 0L);
        log.debug("Queued url (Host: {}, Url: {})", host, url);
    }
    // [END on_message]

    // [BEGIN on_timer]
    @Override
    public void onTimer(Timer timer, OutputCollector output, RuntimeContext ctx) {
        StateAccessor<HostState> accessor = ctx.getState(HOST_STATE, timer);
        var stateOpt = accessor.get();
        if (stateOpt.isEmpty() || stateOpt.get().getPendingUrls().isEmpty()) {
            accessor.clear();
            return;
        }
        HostState state = stateOpt.get();
        String host = state.getHost();

        for (String url : new ArrayList<>(state.getPendingUrls())) {
            String data = processUrl(url);
            ProcessedUrl processedUrl = new ProcessedUrl(host, url, data);
            output.addMessage(new Message("processed_urls", processedUrl));
            log.debug("Processed url (Host: {}, Url: {})", host, url);
        }
        accessor.clear();
    }
    // [END on_timer]

    private String processUrl(String url) {
        long digits = url.chars().filter(Character::isDigit).count();
        return "length: " + url.length() + ", digits: " + digits;
    }
}
