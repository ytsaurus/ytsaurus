package tech.ytsaurus.client.request;

import java.util.Optional;

import javax.annotation.Nullable;

import tech.ytsaurus.rpcproxy.TQueryStatistics;

/**
 * Immutable long aggregate.
 *
 * @see tech.ytsaurus.client.request.QueryStatistics
 */
public class LongAggregate {
    @Nullable
    private final Long total;
    @Nullable
    private final Long max;
    @Nullable
    private final String argmaxNode;

    LongAggregate(Long value) {
        this.total = value;
        this.max = value;
        this.argmaxNode = null;
    }

    LongAggregate(TQueryStatistics.TAggregate aggregate) {
        this.total = aggregate.hasTotal() ? aggregate.getTotal() : null;
        this.max = aggregate.hasMax() ? aggregate.getTotal() : null;
        this.argmaxNode = aggregate.hasArgmaxNode() ? aggregate.getArgmaxNode() : null;
    }

    public Optional<Long> getTotal() {
        return Optional.ofNullable(total);
    }

    public Optional<Long> getMax() {
        return Optional.ofNullable(max);
    }

    public Optional<String> getArgmaxNode() {
        return Optional.ofNullable(argmaxNode);
    }
}
