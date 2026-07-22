package tech.ytsaurus.flow.config;

/**
 * YT Flow Run mode: Worker or Controller.
 * C++ equivalent is the NYT::NFlow::EFlowRunMode.
 */
public enum FlowRunMode {
    Worker(1),
    Controller(2);

    private final int order;

    FlowRunMode(int order) {
        this.order = order;
    }

    public int getOrder() {
        return order;
    }
}
