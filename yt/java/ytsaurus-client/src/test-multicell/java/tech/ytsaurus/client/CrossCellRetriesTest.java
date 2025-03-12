package tech.ytsaurus.client;

import java.time.Duration;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.request.CreateNode;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequestsTestingController;
import tech.ytsaurus.client.rpc.TestingOptions;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.ysontree.YTree;

import static org.hamcrest.MatcherAssert.assertThat;

public class CrossCellRetriesTest extends YTsaurusClientMulticellTestBase {
    private static final String COPY_NODE_METHOD = "CopyNode";
    private static final String MOVE_NODE_METHOD = "MoveNode";

    @Test
    public void testCrossCellCopy() throws Exception {
        testCrossCellCopyMove(COPY_NODE_METHOD);
    }

    @Test
    public void testCrossCellMove() throws Exception {
        testCrossCellCopyMove(MOVE_NODE_METHOD);
    }

    private void testCrossCellCopyMove(String methodName) throws Exception {
        OutageController outageController = new OutageController();
        RpcRequestsTestingController rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setRpcRequestsTestingController(rpcRequestsTestingController)
                .setOutageController(outageController);

        var ytFixture = createYtFixture(new RpcOptions()
                .setTestingOptions(testingOptions)
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO)
                .setRetryPolicyFactory(() ->
                        RetryPolicy.attemptLimited(3, RetryPolicy.forCodes(100))
                ));

        var yt = ytFixture.getYt();

        var portalEntrance = ytFixture.getTestDirectory().child("some-portal");
        var portalCellID = 2;
        yt.createNode(
                CreateNode.builder()
                        .setPath(portalEntrance)
                        .setType(CypressNodeType.PORTAL_ENTRANCE)
                        .setAttributes(Map.of("exit_cell_tag", YTree.integerNode(portalCellID)))
                        .build()
        ).join();

        var srcPath = portalEntrance.child(String.format("src-table-%s", GUID.create()));
        yt.createNode(CreateNode.builder().setPath(srcPath).setType(CypressNodeType.TABLE).build()).join();

        var dstPath = ytFixture.getTestDirectory().child(String.format("dst-table-%s", GUID.create()));

        var error100 = new YTsaurusError(
                TError.newBuilder().setCode(100).setMessage("test error").build()
        );
        rpcRequestsTestingController.clear();
        outageController.addFails(methodName, 1, error100);

        CompletableFuture<GUID> result;
        switch (methodName) {
            case COPY_NODE_METHOD:
                result = yt.copyNode(srcPath.toString(), dstPath.toString());
                break;
            case MOVE_NODE_METHOD:
                result = yt.moveNode(srcPath.toString(), dstPath.toString());
                break;
            default:
                throw new IllegalArgumentException(String.format("illegal method name: %s", methodName));
        }

        result.get(15, TimeUnit.SECONDS);

        var requests = rpcRequestsTestingController.getRequestsByMethod(methodName);
        assertThat("Two fail, one ok", requests.size() == 3);
        requests.sort(Comparator.comparingLong(request -> request.getHeader().getStartTime()));

        assertThat("Different request ids", requests.stream()
                .map(RpcRequestsTestingController.CapturedRequest::getHeader)
                .map(TRequestHeader::getRequestId)
                .distinct().count() == 3
        );

        assertThat("All requests without retry in header", requests.stream()
                .map(RpcRequestsTestingController.CapturedRequest::getHeader)
                .noneMatch(TRequestHeader::getRetry)
        );

        assertThat("Destination exists", yt.existsNode(dstPath.toString()).join());

        if (methodName.equals(MOVE_NODE_METHOD)) {
            assertThat("Source does not exist", !yt.existsNode(srcPath.toString()).join());
        }
    }
}
