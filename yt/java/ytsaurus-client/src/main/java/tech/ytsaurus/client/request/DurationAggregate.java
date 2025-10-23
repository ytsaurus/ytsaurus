package tech.ytsaurus.client.request;

import java.time.Duration;
import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.rpcproxy.TQueryStatistics;

/**
 * Immutable duration aggregate.
 *
 * @see tech.ytsaurus.client.request.QueryStatistics
 */
public class DurationAggregate {
    @Nullable
    private final Duration total;
    @Nullable
    private final Duration max;
    @Nullable
    private final String argmaxNode;

    DurationAggregate(Long value) {
        this.total = RpcUtil.durationFromMicros(value);
        this.max = RpcUtil.durationFromMicros(value);
        this.argmaxNode = null;
    }

    DurationAggregate(TQueryStatistics.TAggregate aggregate) {
        this.total = aggregate.hasTotal()
                ? RpcUtil.durationFromMicros(aggregate.getTotal())
                : null;
        this.max = aggregate.hasMax()
                ? RpcUtil.durationFromMicros(aggregate.getTotal())
                : null;
        this.argmaxNode = aggregate.hasArgmaxNode()
                ? aggregate.getArgmaxNode()
                : null;
    }

    public Optional<Duration> getTotal() {
        return Optional.ofNullable(total);
    }

    public Optional<Duration> getMax() {
        return Optional.ofNullable(max);
    }

    public Optional<String> getArgmaxNode() {
        return Optional.ofNullable(argmaxNode);
    }
}
