package tech.ytsaurus.flow.computation;

/**
 * NYT::NFlow::NCompanion::ECompanionComputationType.
 */
public enum ComputationType {
    Source(0),
    Transform(1);

    private final int order;

    ComputationType(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }
}
