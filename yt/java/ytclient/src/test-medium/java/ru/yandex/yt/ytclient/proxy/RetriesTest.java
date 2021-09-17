package ru.yandex.yt.ytclient.proxy;

import java.time.Duration;
import java.util.Comparator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import ru.yandex.yt.TError;
import ru.yandex.yt.rpcproxy.TReqCreateNode;
import ru.yandex.yt.ytclient.proxy.request.ObjectType;
import ru.yandex.yt.ytclient.rpc.RpcError;
import ru.yandex.yt.ytclient.rpc.RpcFailoverPolicy;
import ru.yandex.yt.ytclient.rpc.RpcOptions;
import ru.yandex.yt.ytclient.rpc.RpcRequestsTestingController;
import ru.yandex.yt.ytclient.rpc.TestingOptions;

import static org.hamcrest.MatcherAssert.assertThat;

public class RetriesTest extends YtClientTestBase {
    @Test
    public void testExistsRetry() throws Exception {
        OutageController outageController = new OutageController();
        RpcRequestsTestingController rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setRpcRequestsTestingController(rpcRequestsTestingController)
                .setOutageController(outageController);

        var error100 = new RpcError(
                TError.newBuilder().setCode(100).build()
        );

        var ytFixture = createYtFixture(new RpcOptions()
                .setTestingOptions(testingOptions)
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO)
                .setFailoverPolicy(new RpcFailoverPolicy() {
                    @Override
                    public boolean onError(Throwable error) {
                        if (error instanceof RpcError) {
                            return true;
                        }
                        return false;
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
        {
            var tablePath = ytFixture.testDirectory.child("static-table-1");

            rpcRequestsTestingController.clear();
            outageController.addFails("ExistsNode", 1, error100);

            yt.existsNode(tablePath.toString()).get(2, TimeUnit.SECONDS);

            var existsRequests = rpcRequestsTestingController.getRequestsByMethod("ExistsNode");
            assertThat("One fail, one ok", existsRequests.size() == 2);
            existsRequests.sort(Comparator.comparingLong(request -> request.getLeft().getStartTime()));

            assertThat("Different request ids",
                    !existsRequests.get(0).getLeft().getRequestId().equals(existsRequests.get(1).getLeft().getRequestId()));

            assertThat("First without retry", !existsRequests.get(0).getLeft().getRetry());
            assertThat("Second with retry", existsRequests.get(1).getLeft().getRetry());
        }
    }

    @Test
    public void testCreateRetry() throws Exception {
        OutageController outageController = new OutageController();
        RpcRequestsTestingController rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setRpcRequestsTestingController(rpcRequestsTestingController)
                .setOutageController(outageController);

        var error100 = new RpcError(
                TError.newBuilder().setCode(100).build()
        );

        var ytFixture = createYtFixture(new RpcOptions()
                .setTestingOptions(testingOptions)
                .setMinBackoffTime(Duration.ZERO)
                .setMaxBackoffTime(Duration.ZERO)
                .setFailoverPolicy(new RpcFailoverPolicy() {
                    @Override
                    public boolean onError(Throwable error) {
                        if (error instanceof RpcError) {
                            return true;
                        }
                        return false;
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
        {
            var tablePath = ytFixture.testDirectory.child("static-table-1");

            rpcRequestsTestingController.clear();
            outageController.addFails("CreateNode", 1, error100);

            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            var createRequests = rpcRequestsTestingController.getRequestsByMethod("CreateNode");
            assertThat("One fail, one ok", createRequests.size() == 2);
            createRequests.sort(Comparator.comparingLong(request -> request.getLeft().getStartTime()));

            assertThat("Different request ids",
                    !createRequests.get(0).getLeft().getRequestId().equals(createRequests.get(1).getLeft().getRequestId()));

            TReqCreateNode firstCreateBody = (TReqCreateNode) createRequests.get(0).getRight();
            TReqCreateNode secondCreateBody = (TReqCreateNode) createRequests.get(1).getRight();
            assertThat("Same mutation id", firstCreateBody.getMutatingOptions().getMutationId().equals(
                    secondCreateBody.getMutatingOptions().getMutationId()));

            assertThat("First without retry", !createRequests.get(0).getLeft().getRetry());
            assertThat("Second with retry", createRequests.get(1).getLeft().getRetry());
        }

        // One successfully retry after two fails.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-2");

            rpcRequestsTestingController.clear();
            outageController.addFails("CreateNode", 2, error100);

            yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);

            var createRequests = rpcRequestsTestingController.getRequestsByMethod("CreateNode");
            assertThat("Two fails, one ok", createRequests.size() == 3);
            createRequests.sort(Comparator.comparingLong(request -> request.getLeft().getStartTime()));

            assertThat("Different request ids",
                    !createRequests.get(0).getLeft().getRequestId().equals(createRequests.get(1).getLeft().getRequestId()));
            assertThat("Different request ids",
                    !createRequests.get(0).getLeft().getRequestId().equals(createRequests.get(2).getLeft().getRequestId()));
            assertThat("Different request ids",
                    !createRequests.get(1).getLeft().getRequestId().equals(createRequests.get(2).getLeft().getRequestId()));

            TReqCreateNode firstCreateBody = (TReqCreateNode) createRequests.get(0).getRight();
            TReqCreateNode secondCreateBody = (TReqCreateNode) createRequests.get(1).getRight();
            TReqCreateNode thirdCreateBody = (TReqCreateNode) createRequests.get(2).getRight();
            assertThat("Same mutation id", firstCreateBody.getMutatingOptions().getMutationId().equals(
                    secondCreateBody.getMutatingOptions().getMutationId()) &&
                    firstCreateBody.getMutatingOptions().getMutationId().equals(
                            thirdCreateBody.getMutatingOptions().getMutationId())
            );

            assertThat("First without retry", !createRequests.get(0).getLeft().getRetry());
            assertThat("Second with retry", createRequests.get(1).getLeft().getRetry());
            assertThat("Third with retry", createRequests.get(2).getLeft().getRetry());
        }

        // Three fails.
        {
            var tablePath = ytFixture.testDirectory.child("static-table-3");

            rpcRequestsTestingController.clear();
            outageController.addFails("CreateNode", 3, error100);

            try {
                yt.createNode(tablePath.toString(), ObjectType.Table).get(2, TimeUnit.SECONDS);
                assertThat("Shouldn't have got here", false);
            } catch (ExecutionException ex) {
            }

            var createRequests = rpcRequestsTestingController.getRequestsByMethod("CreateNode");
            assertThat("Three fails", createRequests.size() == 3);
            createRequests.sort(Comparator.comparingLong(request -> request.getLeft().getStartTime()));

            assertThat("Different request ids",
                    !createRequests.get(0).getLeft().getRequestId().equals(createRequests.get(1).getLeft().getRequestId()));
            assertThat("Different request ids",
                    !createRequests.get(0).getLeft().getRequestId().equals(createRequests.get(2).getLeft().getRequestId()));
            assertThat("Different request ids",
                    !createRequests.get(1).getLeft().getRequestId().equals(createRequests.get(2).getLeft().getRequestId()));

            TReqCreateNode firstCreateBody = (TReqCreateNode) createRequests.get(0).getRight();
            TReqCreateNode secondCreateBody = (TReqCreateNode) createRequests.get(1).getRight();
            TReqCreateNode thirdCreateBody = (TReqCreateNode) createRequests.get(2).getRight();
            assertThat("Same mutation id", firstCreateBody.getMutatingOptions().getMutationId().equals(
                    secondCreateBody.getMutatingOptions().getMutationId()) &&
                    firstCreateBody.getMutatingOptions().getMutationId().equals(
                            thirdCreateBody.getMutatingOptions().getMutationId())
            );

            assertThat("First without retry", !createRequests.get(0).getLeft().getRetry());
            assertThat("Second with retry", createRequests.get(1).getLeft().getRetry());
            assertThat("Third with retry", createRequests.get(2).getLeft().getRetry());
        }
    }

    @Test
    public void testBackoffDuration() {
        OutageController outageController = new OutageController();
        RpcRequestsTestingController rpcRequestsTestingController = new RpcRequestsTestingController();
        TestingOptions testingOptions = new TestingOptions()
                .setRpcRequestsTestingController(rpcRequestsTestingController)
                .setOutageController(outageController);

        var error100 = new RpcError(
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
