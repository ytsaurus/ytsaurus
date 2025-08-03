#include <library/cpp/testing/unittest/registar.h>

#include <yt/yql/providers/yt/fmr/coordinator/client/yql_yt_coordinator_client.h>
#include <yt/yql/providers/yt/fmr/coordinator/impl/yql_yt_coordinator_impl.h>
#include <yt/yql/providers/yt/fmr/coordinator/interface/proto_helpers/yql_yt_coordinator_proto_helpers.h>
#include <yt/yql/providers/yt/fmr/coordinator/server/yql_yt_coordinator_server.h>
#include <yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file/yql_yt_file_coordinator_service.h>
#include <yt/yql/providers/yt/fmr/proto/coordinator.pb.h>

namespace NYql::NFmr {

TDownloadOperationParams downloadOperationParams{
    .Input = TYtTableRef{"Path","Cluster", "FilePath"},
    .Output = TFmrTableRef{{"Cluster", "Path"}}
};

TStartOperationRequest CreateOperationRequest(ETaskType taskType = ETaskType::Download, TOperationParams operationParams = downloadOperationParams) {
    return TStartOperationRequest{
        .TaskType = taskType,
        .OperationParams = operationParams,
        .IdempotencyKey = "IdempotencyKey",
        .ClusterConnections = {
            {TFmrTableId("Cluster", "Path"), TClusterConnection{.TransactionId = "transaction_id", .YtServerName = "fake.yt.yandex.net", .Token = "token"}}
        } // TODO - для file gateway не нужны clusterConnections, можно убрать
    };
}

Y_UNIT_TEST_SUITE(CoordinatorServerTests) {
    Y_UNIT_TEST(SendStartOperationRequestToCoordinatorServer) {
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        ui16 port = 7001;
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
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        ui16 port = 7002;
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
        auto coordinator = MakeFmrCoordinator(TFmrCoordinatorSettings(), MakeFileYtCoordinatorService());
        ui16 port = 7003;
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
