package tech.ytsaurus.client.rpc;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import tech.ytsaurus.client.OutageController;
import tech.ytsaurus.lang.NonNullFields;

@NonNullFields
public class TestingOptions {
    @Nullable
    private OutageController outageController;
    @Nullable
    private RpcRequestsTestingController rpcRequestsTestingController;

    /**
     * @return controller if the client wants to enable the ability to have programmable errors.
     */
    public @Nullable
    OutageController getOutageController() {
        return outageController;
    }

    /**
     * Allows simulating rpc errors using controller.
     * @return self
     */
    public TestingOptions setOutageController(@Nonnull OutageController controller) {
        this.outageController = controller;
        return this;
    }

    /**
     * @return requests testing controller.
     */
    public @Nullable
    RpcRequestsTestingController getRpcRequestsTestingController() {
        return this.rpcRequestsTestingController;
    }

    /**
     * Allows to get sent requests.
     *
     * @return self
     */
    public TestingOptions setRpcRequestsTestingController(
            @Nonnull RpcRequestsTestingController rpcRequestsTestingController) {
        this.rpcRequestsTestingController = rpcRequestsTestingController;
        return this;
    }
}
