#include <yql/tools/yqlworker/executor/subproc/data_protocol.h>
#include <yql/tools/yqlworker/core/inspector_status_pusher.h>

#include <library/cpp/messagebus/ybus.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <yql/essentials/utils/log/log.h>


using namespace NYql;
using namespace NProto;
using namespace NThreading;

struct TClientServer: public NSubprocessTaskExecutor::IDataServerHandler, public IInspectorClient {
    NLog::YqlLoggerScope Logger;
    NSubprocessTaskExecutor::TDataServer DataServer;
    NSubprocessTaskExecutor::TDataClient DataClient;
    ITaskWatcherPtr TaskWatcher;
    NBus::TNetAddr FakeAddress;
    ui32 RevisionToAcknowledge = 0;
    TAtomicCounter AcknowledgedRevisions;
    std::atomic<ui32> LastAcknowledgedRevision = 0;

    enum {
        ANY_REVISION = std::numeric_limits<ui32>::max(),
    };

public:
    TClientServer()
        : Logger(&Cerr)
        , DataServer(NBus::BindOnPort(0, false).second, 4, this)
        , DataClient(DataServer.ActualPort())
        , FakeAddress("localhost", 0)
    {
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Net, NLog::ELevel::TRACE);
        NLog::YqlLogger().SetComponentLevel(NLog::EComponent::Default, NLog::ELevel::TRACE);
        TaskWatcher = MakeInspectorStatusPusher(
            "workerId", FakeAddress, "taskId", "username", TTaskResultChanges::MAX_VALUE, *this, nullptr, nullptr);
    }

    ~TClientServer() {
        DataServer.Shutdown();
    }

    void SendStatusUpdate(
        const NAddr::IRemoteAddr&, NProto::TStatusUpdateRequest&& request, TStatusUpdateCallback callback) override
    {
        if (RevisionToAcknowledge == 0) {
            YQL_LOG(INFO) << "Nothing to acknowledge";
            return;
        }
        if (RevisionToAcknowledge != ANY_REVISION &&
            RevisionToAcknowledge != request.GetResult().GetRevision())
        {
            YQL_LOG(INFO) << "Will not acknowledge revision " << request.GetResult().GetRevision();
            return;
        }
        LastAcknowledgedRevision = request.GetResult().GetRevision();
        AcknowledgedRevisions.Inc();
        NProto::TStatusUpdateResponse response;
        response.SetStatus(NYql::NProto::TStatusUpdateResponse_EStatus_OK);
        callback(FakeAddress, std::move(response));
    }

    void NotifyTaskSubscription(
        const NAddr::IRemoteAddr&, NProto::TTaskSubscriptionNotifyRequest&&) override
    {
        Y_ABORT("must not be called");
    }

    TFuture<void> SendTaskWatchEvent(TTaskResultChanges::TMask changesMask, ui32 revision) {
        TTaskWatchEventRequest request;
        request.SetEvent(TTaskWatchEventRequest::RESULTS_CHANGED);
        request.SetChangesMask(changesMask);
        NProto::TTaskResult* changes = request.MutableChanges();
        changes->SetRevision(revision);
        return DataClient.SendTaskWatchEvent(std::move(request));
    }

    void OnTaskWatchEvent(TTaskWatchEventRequest&& request, NBus::TOnMessageContext* msgCtx) override {
        UNIT_ASSERT_EQUAL(TTaskWatchEventRequest::RESULTS_CHANGED, request.GetEvent());
        TaskWatcher->OnResultChange(
            request.GetChangesMask(), TTaskResultPtr(request.release_changes()), request.GetPid(), msgCtx);
    }

    TGetTaskResponse OnGetTask(TGetTaskRequest&&) override {
        Y_ABORT("must not be called");
    }

    void OnMetricsRegistrySnapshot(TMetricsRegistrySnapshotRequest&&) override {
        Y_ABORT("must not be called");
    }

    void SendHeartbeatToAll(NProto::THeartbeatRequest&&, THeartbeatCallback) override {
        Y_ABORT("must not be called");
    }

    void CloseConnectionWith(const NAddr::IRemoteAddr&) override {
        Y_ABORT("must not be called");
    }

    size_t GetConnectionsCount() override {
        Y_ABORT("must not be called");
    }

    void WaitAllMessagesDelivered() const override {
        Y_ABORT("must not be called");
    }

};

Y_UNIT_TEST_SUITE(TInspectorStatusPusherTest) {

    Y_UNIT_TEST(TaskWatchEvent) {
        TClientServer clientServer;
        UNIT_ASSERT(clientServer.TaskWatcher->WillWatch(TTaskResultChanges::MAX_VALUE));
        ui32 revision = 1;
        clientServer.RevisionToAcknowledge = revision;
        clientServer.SendTaskWatchEvent(TTaskResultChanges::MAX_VALUE, revision).Wait();
        UNIT_ASSERT_VALUES_EQUAL(revision, clientServer.LastAcknowledgedRevision.load());
        UNIT_ASSERT_VALUES_EQUAL(1, clientServer.AcknowledgedRevisions.Val());
    }

    Y_UNIT_TEST(NotAcknowledged) {
        TClientServer clientServer;
        clientServer.RevisionToAcknowledge = 2;
        UNIT_ASSERT(!clientServer.SendTaskWatchEvent(TTaskResultChanges::MAX_VALUE, 1).Wait(TDuration::Seconds(10)));
        clientServer.SendTaskWatchEvent(TTaskResultChanges::MAX_VALUE, 2).Wait();
        UNIT_ASSERT(!clientServer.SendTaskWatchEvent(TTaskResultChanges::MAX_VALUE, 3).Wait(TDuration::Seconds(10)));
        UNIT_ASSERT_VALUES_EQUAL(2, clientServer.LastAcknowledgedRevision.load());
        UNIT_ASSERT_VALUES_EQUAL(1, clientServer.AcknowledgedRevisions.Val());
    }

    Y_UNIT_TEST(AcknowledgeAll) {
        TClientServer clientServer;
        clientServer.RevisionToAcknowledge = TClientServer::ANY_REVISION;
        TVector<TFuture<void>> futures;
        for (int revision = 1; revision <= 100; ++revision) {
            futures.push_back(clientServer.SendTaskWatchEvent(TTaskResultChanges::MAX_VALUE, revision));
        }
        WaitAll(futures).Wait(TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(100, clientServer.AcknowledgedRevisions.Val());
        UNIT_ASSERT_GT(clientServer.LastAcknowledgedRevision.load(), 0);
        UNIT_ASSERT(futures[clientServer.LastAcknowledgedRevision.load() - 1].HasValue());
    }

}
