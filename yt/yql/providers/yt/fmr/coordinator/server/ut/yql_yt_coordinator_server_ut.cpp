#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers/yql_yt_coordinator_proto_helpers.h>
#include <yt/yql/providers/yt/fmr/coordinator/server/yql_yt_coordinator_server.h>
#include <yt/yql/providers/yt/fmr/proto/coordinator.pb.h>

namespace NYql::NFmr {

TDownloadTaskParams downloadTaskParams{
    .Input = TYtTableRef{"Path","Cluster","TransactionId"},
    .Output = TFmrTableRef{"TableId"}
};

TStartOperationRequest CreateOperationRequest(ETaskType taskType = ETaskType::Download, TTaskParams taskParams = downloadTaskParams) {
    return TStartOperationRequest{.TaskType = taskType, .TaskParams = taskParams, .SessionId = "SessionId", .IdempotencyKey = "IdempotencyKey"};
}

Y_UNIT_TEST_SUITE(CoordinatorServerTests) {
    Y_UNIT_TEST(SendStartOperationRequestToCoordinatorServer) {
        auto coordinator = MakeFmrCoordinator();
        ui16 port = 7000;
        TFmrCoordinatorServerSettings coordinatorServerSettings{.Port = port};
        auto coordinatorServer = MakeFmrCoordinatorServer(coordinator, coordinatorServerSettings);
        coordinatorServer->Start();

        TFmrCoordinatorClientSettings coordinatorClientSettings{.Port = port};
        auto coordinatorClient = MakeFmrCoordinatorClient(coordinatorClientSettings);

        auto startOperationRequest = CreateOperationRequest();
        auto startOperationResponse = coordinatorClient->StartOperation(startOperationRequest).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(startOperationResponse.Status, EOperationStatus::Accepted);
    }

    Y_UNIT_TEST(SendGetOperationRequestToCoordinatorServer) {
        auto coordinator = MakeFmrCoordinator();
        ui16 port = 7000;
        TFmrCoordinatorServerSettings coordinatorServerSettings{.Port = port};
        auto coordinatorServer = MakeFmrCoordinatorServer(coordinator, coordinatorServerSettings);
        coordinatorServer->Start();

        TFmrCoordinatorClientSettings coordinatorClientSettings{.Port = port};
        auto coordinatorClient = MakeFmrCoordinatorClient(coordinatorClientSettings);
        auto startOperationResponse = coordinatorClient->StartOperation(CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;
        auto getOperationResponse = coordinatorClient->GetOperation({operationId}).GetValueSync();
        EOperationStatus status = getOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Accepted);
    }

    Y_UNIT_TEST(SendDeleteOperationRequestToCoordinatorServer) {
        auto coordinator = MakeFmrCoordinator();
        ui16 port = 7000;
        TFmrCoordinatorServerSettings coordinatorServerSettings{.Port = port};
        auto coordinatorServer = MakeFmrCoordinatorServer(coordinator, coordinatorServerSettings);
        coordinatorServer->Start();

        TFmrCoordinatorClientSettings coordinatorClientSettings{.Port = port};
        auto coordinatorClient = MakeFmrCoordinatorClient(coordinatorClientSettings);

        auto startOperationResponse = coordinatorClient->StartOperation(CreateOperationRequest()).GetValueSync();
        TString operationId = startOperationResponse.OperationId;
        auto deleteOperationResponse = coordinatorClient->DeleteOperation({operationId}).GetValueSync();
        EOperationStatus status = deleteOperationResponse.Status;
        UNIT_ASSERT_VALUES_EQUAL(status, EOperationStatus::Aborted);
    }

    // TODO - add more tests, check for error handling
}

} // namespace NYql::NFmr
