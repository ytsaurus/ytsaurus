package tech.ytsaurus.client.operations;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.client.YTsaurusClientTestBase;
import tech.ytsaurus.client.request.MergeOperation;
import tech.ytsaurus.client.request.StartTransaction;
import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequestsTestingController;
import tech.ytsaurus.client.rpc.RpcUtil;
import tech.ytsaurus.client.rpc.TestingOptions;
import tech.ytsaurus.core.cypress.CypressNodeType;
import tech.ytsaurus.rpcproxy.TReqCreateNode;

public class MergeOperationTest extends YTsaurusClientTestBase {
    @Test
    public void testTransactional() {
        var controller = new RpcRequestsTestingController();
        var testingOptions = new TestingOptions().setRpcRequestsTestingController(controller);
        var ytFixture = createYtFixture(new RpcOptions().setTestingOptions(testingOptions));
        var yt = ytFixture.getYt();
        var inputTable = ytFixture.getTestDirectory().child("input-table");
        var outputTable = ytFixture.getTestDirectory().child("output-table");

        var transaction = yt.startTransaction(new StartTransaction(TransactionType.Master)).join();

        transaction.createNode(inputTable.toString(), CypressNodeType.TABLE).join();

        transaction.startMerge(MergeOperation.builder()
                .setSpec(new MergeSpec(List.of(inputTable), outputTable)).build()).join();

        var createRequest = controller.getRequestsByMethod("CreateNode").get(1);
        TReqCreateNode createBody = (TReqCreateNode) createRequest.getBody();
        Assert.assertEquals(createBody.getTransactionalOptions().getTransactionId(),
                RpcUtil.toProto(transaction.getId()));
    }
}
