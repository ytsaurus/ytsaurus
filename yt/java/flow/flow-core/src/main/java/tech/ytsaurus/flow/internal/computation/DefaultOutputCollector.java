package tech.ytsaurus.flow.internal.computation;

import java.util.List;

import org.jspecify.annotations.Nullable;
import tech.ytsaurus.flow.computation.OutputCollector;
import tech.ytsaurus.flow.computation.TransformResult;
import tech.ytsaurus.flow.row.Message;
import tech.ytsaurus.flow.row.NewTimer;

/**
 * A default implementation of {@link OutputCollector} which collects output messages into a {@link TransformResult}
 * object.
 *
 * @see OutputCollector
 * @see RootCollector
 */
public class DefaultOutputCollector implements OutputCollector {
    private final TransformResult transformResult;
    private final RootCollector rootCollector;
    private boolean registered;

    public DefaultOutputCollector(RootCollector rootCollector, TransformResult transformResult) {
        this.transformResult = transformResult;
        this.rootCollector = rootCollector;
    }

    /**
     * @see OutputCollector#setParentIds(List)
     */
    @Override
    public OutputCollector setParentIds(List<String> parentIds) {
        return rootCollector.setParentIds(parentIds);
    }

    /**
     * @see OutputCollector#addMessage(Message, boolean)
     */
    @Override
    public void addMessage(Message message, boolean distribute) {
        ensureRegistered();
        transformResult.addMessage(message, distribute);
    }

    /**
     * @see OutputCollector#addTimer(String, long, long)
     */
    @Override
    public void addTimer(@Nullable String timerStreamId, long triggerTimestamp, long eventTimestamp) {
        ensureRegistered();
        transformResult.addTimer(new NewTimer(triggerTimestamp, eventTimestamp, timerStreamId));
    }

    private void ensureRegistered() {
        if (!registered) {
            rootCollector.register(transformResult);
            registered = true;
        }
    }
}
