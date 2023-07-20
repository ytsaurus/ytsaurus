package tech.ytsaurus.client;

import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.client.rpc.RpcFailoverPolicy;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequestsTestingController;
import tech.ytsaurus.client.rpc.TestingOptions;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.rpcproxy.TReqCreateNode;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

public class RetriesTest extends YTsaurusClientTestBase {
    @Test
    public void testExistsRetry() throws Exception {
        OutageController outageController = new OutageController();
        RpcRequestsTestingController rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setRpcRequestsTestingController(rpcRequestsTestingController)
                .setOutageController(outageController);

        var error100 = new YTsaurusError(
                TError.newBuilder().setCode(100).build()
        );

        var ytFixture = createYtFixture(new RpcOptions()
                .setTestingOptions(testingOptions)
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO)
                .setFailoverPolicy(new RpcFailoverPolicy() {
                    @Override
                    public boolean onError(Throwable error) {
                        return error instanceof YTsaurusError;
                    }

                    @Override
                    public boolean onTimeout() {
                        return false;
                    }

                    @Override
                    public boolean randomizeDcs() {
                        return false;
                    }
                }));

        var yt = ytFixture.yt;

        // One successfully retry after fail.

        var tablePath = ytFixture.testDirectory.child("static-table-1");

        rpcRequestsTestingController.clear();
        outageController.addFails("ExistsNode", 1, error100);

        yt.existsNode(tablePath.toString()).get(2, TimeUnit.SECONDS);

        var existsRequests = rpcRequestsTestingController.getRequestsByMethod("ExistsNode");
        assertThat("One fail, one ok", existsRequests.size() == 2);
        existsRequests.sort(Comparator.comparingLong(request -> request.getHeader().getStartTime()));

        assertThat("Different request ids",
                !existsRequests.get(0).getHeader().getRequestId()
                        .equals(existsRequests.get(1).getHeader().getRequestId())
        );

        assertThat("First without retry", !existsRequests.get(0).getHeader().getRetry());
        assertThat("Second with retry", existsRequests.get(1).getHeader().getRetry());
    }

    @Test
    public void testCreateRetry() throws Exception {
        OutageController outageController = new OutageController();
        RpcRequestsTestingController rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setRpcRequestsTestingController(rpcRequestsTestingController)
                .setOutageController(outageController);

        var error100 = new YTsaurusError(
                TError.newBuilder().setCode(100).build()
        );

        var ytFixture = createYtFixture(new RpcOptions()
                .setTestingOptions(testingOptions)
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO)
                .setFailoverPolicy(new RpcFailoverPolicy() {
                    @Override
                    public boolean onError(Throwable error) {
                        return error instanceof YTsaurusError;
                    }

                    @Override
                    public boolean onTimeout() {
                        return false;
                    }

                    @Override
                    public boolean randomizeDcs() {
                        return false;
                    }
                }));

        var yt = ytFixture.yt;

        // One successfully retry after fail.
        var firstTablePath = ytFixture.testDirectory.child("static-table-1");

        rpcRequestsTestingController.clear();
        outageController.addFails("CreateNode", 1, error100);

        yt.createNode(firstTablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

        var firstCreateRequests = rpcRequestsTestingController.getRequestsByMethod("CreateNode");
        assertThat("One fail, one ok", firstCreateRequests.size() == 2);
        firstCreateRequests.sort(Comparator.comparingLong(request -> request.getHeader().getStartTime()));

        assertThat("Different request ids",
                !firstCreateRequests.get(0).getHeader().getRequestId()
                        .equals(firstCreateRequests.get(1).getHeader().getRequestId()));

        TReqCreateNode firstCreateBodyAfterOneFail = (TReqCreateNode) firstCreateRequests.get(0).getBody();
        TReqCreateNode secondCreateBodyAfterOneFail = (TReqCreateNode) firstCreateRequests.get(1).getBody();
        assertThat("Same mutation id", firstCreateBodyAfterOneFail.getMutatingOptions().getMutationId().equals(
                secondCreateBodyAfterOneFail.getMutatingOptions().getMutationId()));

        assertThat("First without retry", !firstCreateRequests.get(0).getHeader().getRetry());
        assertThat("Second with retry", firstCreateRequests.get(1).getHeader().getRetry());

        // One successfully retry after two fails.
        var secondTablePath = ytFixture.testDirectory.child("static-table-2");

        rpcRequestsTestingController.clear();
        outageController.addFails("CreateNode", 2, error100);

        yt.createNode(secondTablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS);

        var secondCreateRequests = rpcRequestsTestingController.getRequestsByMethod("CreateNode");
        assertThat("Two fails, one ok", secondCreateRequests.size() == 3);
        secondCreateRequests.sort(Comparator.comparingLong(request -> request.getHeader().getStartTime()));

        assertThat("Different request ids",
                !secondCreateRequests.get(0).getHeader().getRequestId()
                        .equals(secondCreateRequests.get(1).getHeader().getRequestId()));
        assertThat("Different request ids",
                !secondCreateRequests.get(0).getHeader().getRequestId()
                        .equals(secondCreateRequests.get(2).getHeader().getRequestId()));
        assertThat("Different request ids",
                !secondCreateRequests.get(1).getHeader().getRequestId()
                        .equals(secondCreateRequests.get(2).getHeader().getRequestId()));

        TReqCreateNode firstCreateBodyAfterTwoFails = (TReqCreateNode) secondCreateRequests.get(0).getBody();
        TReqCreateNode secondCreateBodyAfterTwoFails = (TReqCreateNode) secondCreateRequests.get(1).getBody();
        TReqCreateNode thirdCreateBodyAfterTwoFails = (TReqCreateNode) secondCreateRequests.get(2).getBody();
        assertThat("Same mutation id", firstCreateBodyAfterTwoFails.getMutatingOptions().getMutationId().equals(
                secondCreateBodyAfterTwoFails.getMutatingOptions().getMutationId()) &&
                firstCreateBodyAfterTwoFails.getMutatingOptions().getMutationId().equals(
                        thirdCreateBodyAfterTwoFails.getMutatingOptions().getMutationId())
        );

        assertThat("First without retry", !secondCreateRequests.get(0).getHeader().getRetry());
        assertThat("Second with retry", secondCreateRequests.get(1).getHeader().getRetry());
        assertThat("Third with retry", secondCreateRequests.get(2).getHeader().getRetry());

        // Three fails.
        var thirdTablePath = ytFixture.testDirectory.child("static-table-3");

        rpcRequestsTestingController.clear();
        outageController.addFails("CreateNode", 3, error100);

        assertThrows(
                ExecutionException.class,
                () -> yt.createNode(thirdTablePath.toString(), CypressNodeType.TABLE).get(2, TimeUnit.SECONDS)
        );

        var thirdCreateRequests = rpcRequestsTestingController.getRequestsByMethod("CreateNode");
        assertThat("Three fails", thirdCreateRequests.size() == 3);
        thirdCreateRequests.sort(Comparator.comparingLong(request -> request.getHeader().getStartTime()));

        assertThat("Different request ids",
                !thirdCreateRequests.get(0).getHeader().getRequestId()
                        .equals(thirdCreateRequests.get(1).getHeader().getRequestId()));
        assertThat("Different request ids",
                !thirdCreateRequests.get(0).getHeader().getRequestId()
                        .equals(thirdCreateRequests.get(2).getHeader().getRequestId()));
        assertThat("Different request ids",
                !thirdCreateRequests.get(1).getHeader().getRequestId()
                        .equals(thirdCreateRequests.get(2).getHeader().getRequestId()));

        TReqCreateNode firstCreateBodyAfterThreeFails = (TReqCreateNode) thirdCreateRequests.get(0).getBody();
        TReqCreateNode secondCreateBodyAfterThreeFails = (TReqCreateNode) thirdCreateRequests.get(1).getBody();
        TReqCreateNode thirdCreateBodyAfterThreeFails = (TReqCreateNode) thirdCreateRequests.get(2).getBody();
        assertThat("Same mutation id", firstCreateBodyAfterThreeFails.getMutatingOptions().getMutationId().equals(
                secondCreateBodyAfterThreeFails.getMutatingOptions().getMutationId()) &&
                firstCreateBodyAfterThreeFails.getMutatingOptions().getMutationId().equals(
                        thirdCreateBodyAfterThreeFails.getMutatingOptions().getMutationId())
        );

        assertThat("First without retry", !thirdCreateRequests.get(0).getHeader().getRetry());
        assertThat("Second with retry", thirdCreateRequests.get(1).getHeader().getRetry());
        assertThat("Third with retry", thirdCreateRequests.get(2).getHeader().getRetry());
    }

    @Test
    public void testBackoffDuration() {
        OutageController outageController = new OutageController();
        RpcRequestsTestingController rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setRpcRequestsTestingController(rpcRequestsTestingController)
                .setOutageController(outageController);

        var error100 = new YTsaurusError(
                TError.newBuilder().setCode(100).build()
        );

        var ytFixture = createYtFixture(new RpcOptions()
                .setTestingOptions(testingOptions)
                .setMinBackoffTime(Duration.ofMillis(500))
                .setMaxBackoffTime(Duration.ofMillis(1000))
                .setRetryPolicyFactory(() -> RetryPolicy.forCodes(100)));

        var yt = ytFixture.yt;

        outageController.addFails("ExistsNode", 2, error100);

        var tablePath = ytFixture.testDirectory.child("static-table");

        long startTime = System.currentTimeMillis();
        yt.existsNode(tablePath.toString()).join();
        long endTime = System.currentTimeMillis();

        assertThat("Expected two retries with postpones", endTime - startTime >= 1000);
    }
}
