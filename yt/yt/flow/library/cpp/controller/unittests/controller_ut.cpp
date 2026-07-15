#include <yt/yt/flow/library/cpp/controller/controller.h>
#include <yt/yt/flow/library/cpp/controller/controller_service.h>
#include <yt/yt/flow/library/cpp/controller/flow_executor.h>

#include <yt/yt/flow/library/cpp/controller/config.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/persisted_state_manager.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/worker_tracker.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/yt_connector.h>

#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/authenticator.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/flow/lib/client/controller/controller_service_proxy.h>

#include <yt/yt/core/actions/invoker_util.h>
#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/test_framework/framework.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NApi;
using namespace NClient::NCache;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using ::testing::_;
using ::testing::Return;
using ::testing::SaveArg;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TFixedClientCache
    : public IClientsCache
{
public:
    explicit TFixedClientCache(IClientPtr client)
        : Client_(std::move(client))
    { }

    IClientPtr GetClient(TStringBuf /*clusterUrl*/) override
    {
        return Client_;
    }

private:
    IClientPtr Client_;
};

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "TestLogger");

////////////////////////////////////////////////////////////////////////////////

struct TTestDatabaseRow
{
    TSequenceId SequenceId;
    EStorageRowFlags Flags;
    TPersistedStateName Name;
    std::string KeyLeft;
    std::string KeyRight;
    std::string Value;
};

struct TTestDatabase
{
    std::map<TSequenceId, TTestDatabaseRow> Data;
    bool DebugDisconnect = false;
};

class TStorageHandler : public TPersistedStateStorageHandlerBase<std::string>
{
public:
    using TStorageRow = typename TPersistedStateStorageHandlerBase<std::string>::TStorageRow;

    TStorageHandler(TTestDatabase& database)
        : Database_{database}
    {
        MaxSelectSize_ = 10;
        MaxExecuteSize_ = 10;
    }

    void Select(TSequenceId lastSequenceId, std::vector<TStorageRow>& result) override
    {
        THROW_ERROR_EXCEPTION_IF(Database_.DebugDisconnect, "Debug disconnect");
        auto it = Database_.Data.upper_bound(lastSequenceId);
        for (ssize_t i = 0; i < MaxSelectSize_ && it != Database_.Data.end(); ++i, ++it) {
            result.emplace_back(TStorageRow{
                it->second.SequenceId,
                it->second.Flags,
                it->second.Name,
                it->second.KeyLeft,
                it->second.KeyRight,
                it->second.Value});
        }
    }

    void Execute(std::vector<TStorageRow>&& insertRows, const std::vector<TSequenceId>& deleteRows, bool, const std::vector<TPersistedStateCommitContext*>&) override
    {
        THROW_ERROR_EXCEPTION_IF(Database_.DebugDisconnect, "Debug disconnect");
        for (auto sequenceId : deleteRows) {
            YT_ASSERT(Database_.Data.contains(sequenceId));
            Database_.Data.erase(sequenceId);
        }
        for (auto& row : insertRows) {
            YT_ASSERT(!Database_.Data.contains(row.SequenceId));
            Database_.Data.emplace(
                row.SequenceId,
                TTestDatabaseRow{row.SequenceId,
                    row.Flags,
                    row.Name,
                    row.KeyLeft,
                    row.KeyRight,
                    row.Value});
        }
    }

private:
    TTestDatabase& Database_;
};

////////////////////////////////////////////////////////////////////////////////

struct TPersistedStateManagerLocalState
    : public TRefCounted
{
    void CheckImportantVersions(const TPipelineImportantVersionsPtr& expectedVersions)
    {
        auto actual = MakePipelineImportantVersions(FlowView->State, Spec);
        actual->FlowCoreTargetVersion = FlowCoreTarget->GetVersion();
        expectedVersions->EnsureEqual(*actual);
    }

    void CheckDynamicImportantVersions(const TVersion& expectedVersion)
    {
        THROW_ERROR_EXCEPTION_UNLESS(
            expectedVersion == DynamicSpec->GetVersion(),
            NFlow::EErrorCode::SpecVersionMismatch,
            "Versions mismatch");
    }

    TSpinLock Lock;
    TTestDatabase Database;
    TIntrusivePtr<TStorageHandler> StorageHandler = New<TStorageHandler>(Database);
    TPersistedStateControlPtr<std::string> PersistedMasterControl = New<TPersistedStateControl<std::string>>(StorageHandler);
    TFlowViewPtr FlowView = New<TFlowView>();
    TVersionedPipelineSpecPtr Spec = New<TVersionedPipelineSpec>();
    TVersionedDynamicPipelineSpecPtr DynamicSpec = New<TVersionedDynamicPipelineSpec>();
    TVersionedFlowCoreTargetPtr FlowCoreTarget = New<TVersionedFlowCoreTarget>();
};

////////////////////////////////////////////////////////////////////////////////

class TControllerTest
    : public ::testing::Test
{
public:
    void Prepare()
    {
        EXPECT_CALL(*YTConnector, SubscribeLeadingStarted(_))
            .WillOnce(SaveArg<0>(&StartLeading));
        EXPECT_CALL(*YTConnector, SubscribeLeadingEnded(_))
            .WillOnce(SaveArg<0>(&StopLeading));

        Controller->Initialize();

        ASSERT_TRUE(StartLeading);
        ASSERT_TRUE(StopLeading);

        // Local PersistedStateManager.
        EXPECT_CALL(*PersistedStateManager, PersistFlowState(_, _))
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] (const TFlowStatePtr& flowState, const TPipelineImportantVersionsPtr& expectedVersions) {
                    auto guard = Guard(state->Lock);
                    flowState->CommitMutation();
                    state->CheckImportantVersions(expectedVersions);
                    state->FlowView->State = CloneYsonStruct(flowState);
                });
        EXPECT_CALL(*PersistedStateManager, RecoverFlowState())
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] () {
                    auto guard = Guard(state->Lock);
                    auto flowState = CloneYsonStruct(state->FlowView->State);
                    state->PersistedMasterControl = New<TPersistedStateControl<std::string>>(state->StorageHandler);
                    flowState->AttachToControl(state->PersistedMasterControl);
                    state->PersistedMasterControl->Recover();
                    return flowState;
                });
        EXPECT_CALL(*PersistedStateManager, PersistSpecs(_, _, _, _))
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] (
                    const std::optional<TVersionedPipelineSpecPtr>& spec,
                    const std::optional<TPipelineImportantVersionsPtr>& expectedVersions,
                    const std::optional<TVersionedDynamicPipelineSpecPtr>& dynamicSpec,
                    const std::optional<TVersion>& expectedDynamicSpecVersion) {
                    auto guard = Guard(state->Lock);
                    if (spec && expectedVersions) {
                        state->CheckImportantVersions(*expectedVersions);
                        state->Spec = CloneYsonStruct(*spec);
                    }
                    if (dynamicSpec && expectedDynamicSpecVersion) {
                        state->CheckDynamicImportantVersions(*expectedDynamicSpecVersion);
                        state->DynamicSpec = CloneYsonStruct(*dynamicSpec);
                    }
                });
        EXPECT_CALL(*PersistedStateManager, RecoverSpec())
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] () {
                    auto guard = Guard(state->Lock);
                    return CloneYsonStruct(state->Spec);
                });
        EXPECT_CALL(*PersistedStateManager, RecoverDynamicSpec())
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] () {
                    auto guard = Guard(state->Lock);
                    return CloneYsonStruct(state->DynamicSpec);
                });

        EXPECT_CALL(*PersistedStateManager, AdvanceInputMessagesWatermark(_))
            .WillRepeatedly(Return());

        EXPECT_CALL(*PersistedStateManager, PersistFlowCoreTarget(_, _))
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] (
                    const TVersionedFlowCoreTargetPtr& flowCoreTarget,
                    const TPipelineImportantVersionsPtr& expectedVersions) {
                    auto guard = Guard(state->Lock);
                    state->CheckImportantVersions(expectedVersions);
                    state->FlowCoreTarget = CloneYsonStruct(flowCoreTarget);
                });
        EXPECT_CALL(*PersistedStateManager, RecoverFlowCoreTarget())
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] () {
                    auto guard = Guard(state->Lock);
                    return CloneYsonStruct(state->FlowCoreTarget);
                });

        auto transaction = New<StrictMock<NApi::TMockTransaction>>();
        EXPECT_CALL(*transaction, SetNode(_, _, _))
            .WillRepeatedly(Return(OKFuture));
        EXPECT_CALL(*transaction, Commit(_))
            .WillRepeatedly(Return(MakeFuture(NApi::TTransactionCommitResult{})));
        EXPECT_CALL(*YTConnector, StartTransaction(_, _))
            .WillRepeatedly(Return(MakeFuture<NApi::ITransactionPtr>(transaction)));
        auto client = New<StrictMock<NApi::TMockClient>>();
        auto timestampProvider = New<StrictMock<NTransactionClient::TMockTimestampProvider>>();
        EXPECT_CALL(*timestampProvider, GenerateTimestamps(_, _))
            .WillRepeatedly(Return(MakeFuture<NTransactionClient::TTimestamp>(0)));
        client->SetTimestampProvider(timestampProvider);
        EXPECT_CALL(*client, AttachTransaction(_, _))
            .WillRepeatedly(Return(NApi::ITransactionPtr(transaction)));
        EXPECT_CALL(*client, StartTransaction(_, _))
            .WillRepeatedly(Return(MakeFuture<NApi::ITransactionPtr>(transaction)));
        EXPECT_CALL(*YTConnector, GetClient())
            .WillRepeatedly(Return(NApi::IClientPtr(client)));
        EXPECT_CALL(*YTConnector, GetClientsCache())
            .WillRepeatedly(Return(New<TFixedClientCache>(NApi::IClientPtr(client))));

        EXPECT_CALL(*YTConnector, IsLeader())
            .WillRepeatedly(Return(true));
        EXPECT_CALL(*YTConnector, GetPipelinePath())
            .WillRepeatedly(Return(NYPath::TRichYPath("cluster://path")));

        EXPECT_CALL(*WorkerTracker, GetWorkers())
            .WillRepeatedly(Return(std::vector<TWorkerInfo>{}));
    }

    template <typename TFunctor>
    void ExecuteViaControlQueue(TFunctor&& functor)
    {
        NConcurrency::WaitFor(
            BIND(functor)
                .AsyncVia(ControlActionQueue->GetInvoker(EControlQueue::Default))
                .Run())
            .ThrowOnError();
    }

    void StartLeadingAndWaitReady()
    {
        StartLeading();

        while (true) {
            try {
                Controller->EnsureIsLeader();
                Y_UNUSED(Controller->GetFlowViewKeeper()->GetFlowView());
                break;
            } catch (const std::exception& ex) {
                YT_LOG_INFO(ex, "Still wait until controller starts leading");
                NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            }
        }
    }

    void SetUp() override
    {
        ControllerConfig = New<TControllerConfig>();
        ControllerConfig->Postprocess();

        PersistedStateManager = New<StrictMock<TMockPersistedStateManager>>();
        WorkerTracker = New<StrictMock<TMockWorkerTracker>>();
        YTConnector = New<StrictMock<TMockYTConnector>>();
        ControlActionQueue = NConcurrency::CreateEnumIndexedFairShareActionQueue<NController::EControlQueue>("TestControl");
        ControllerThreadPool = NConcurrency::CreateFairShareThreadPool(10, "ControllerHeavy");

        PersistedStateManagerLocalState = New<TPersistedStateManagerLocalState>();

        auto authenticator = New<StrictMock<TMockPipelineAuthenticator>>();
        EXPECT_CALL(*authenticator, CreateSelfCredentialsInjectingChannelFactory(_))
            .WillRepeatedly(Return(nullptr));
        EXPECT_CALL(*authenticator, CreateYTControllerRpcAuthenticator())
            .WillRepeatedly(Return(nullptr));

        // CreateFlowExecutor builds shared NTables handles in its ctor; that calls into the YT connector.
        EXPECT_CALL(*YTConnector, GetClient())
            .WillRepeatedly(Return(NApi::IClientPtr{}));
        EXPECT_CALL(*YTConnector, GetPipelinePath())
            .WillRepeatedly(Return(NYPath::TRichYPath("cluster://path")));

        Controller = CreateController(
            ControllerConfig,
            New<TNodeInfo>(),
            WorkerTracker,
            /*throttlerHost*/ nullptr,
            ControllerThreadPool->GetInvoker("SchedulerTest"),
            YTConnector,
            PersistedStateManager,
            authenticator,
            /*ignoreSingletonsDynamicConfig*/ false,
            /*clockClusterTag*/ NObjectClient::InvalidCellTag,
            CreateSyncStatusProfiler());

        LocalServer = NRpc::CreateLocalServer();

        const auto flowExecutor = CreateFlowExecutor(
            Controller,
            PersistedStateManager,
            YTConnector,
            New<TControllerServiceConfig>(),
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            GetSyncInvoker());
        const auto service = CreateControllerService(
            flowExecutor,
            authenticator,
            ControlActionQueue->GetInvoker(NController::EControlQueue::Admin));
        LocalServer->RegisterService(service);
        LocalServer->Start();

        ControllerServiceProxy = std::make_unique<TControllerServiceProxy>(NRpc::CreateLocalChannel(LocalServer));
    }

    void TearDown() override
    {
        YT_UNUSED_FUTURE(LocalServer->Stop());
        ControllerThreadPool->Shutdown();
    }

public:
    TControllerConfigPtr ControllerConfig;
    TMockPersistedStateManagerPtr PersistedStateManager;
    TMockWorkerTrackerPtr WorkerTracker;
    TMockYTConnectorPtr YTConnector;
    TControlActionQueuePtr ControlActionQueue;
    NConcurrency::IFairShareThreadPoolPtr ControllerThreadPool;

    IControllerPtr Controller;
    TCallback<void()> StartLeading;
    TCallback<void()> StopLeading;

    TIntrusivePtr<TPersistedStateManagerLocalState> PersistedStateManagerLocalState;

    NRpc::IServerPtr LocalServer;
    std::unique_ptr<TControllerServiceProxy> ControllerServiceProxy;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, WaitPersist)
{
    Prepare();
    YT_LOG_INFO("Start");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        {
            auto getReq = ControllerServiceProxy->GetDynamicSpec();
            auto getRsp = WaitFor(getReq->Invoke()).ValueOrThrow();

            {
                auto setReq = ControllerServiceProxy->SetDynamicSpec();
                setReq->set_expected_version(getRsp->version());
                setReq->set_spec(getRsp->spec()); // No spec change - no version change.
                auto setRsp = WaitFor(setReq->Invoke()).ValueOrThrow();
                ASSERT_EQ(getRsp->version(), setRsp->version());
            }

            auto setReq = ControllerServiceProxy->SetDynamicSpec();
            setReq->set_expected_version(getRsp->version());
            // Just random option.
            setReq->set_spec(R"({"controller_connector"={"controller_wait_timeout"=1234;};})");
            auto setRsp = WaitFor(setReq->Invoke()).ValueOrThrow();
            ASSERT_EQ(getRsp->version() + 1, setRsp->version());

            auto getDynamicVersion = [state = PersistedStateManagerLocalState] {
                auto guard = Guard(state->Lock);
                return state->FlowView->State->ExecutionSpec->DynamicPipelineSpec->GetVersion().Underlying();
            };

            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
            while (getDynamicVersion() != setRsp->version()) {
                YT_LOG_INFO("Still wait until update of dynamic spec will be available in flow view.");
                NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            }
        }

        {
            i64 targetVersion = -1;
            while (true) {
                try {
                    auto getReq = ControllerServiceProxy->GetSpec();
                    auto getRsp = WaitFor(getReq->Invoke()).ValueOrThrow();

                    {
                        auto setReq = ControllerServiceProxy->SetSpec();
                        setReq->set_expected_version(getRsp->version());
                        setReq->set_spec(getRsp->spec()); // No spec change - no version change.
                        auto setRsp = WaitFor(setReq->Invoke()).ValueOrThrow();
                        ASSERT_EQ(getRsp->version(), setRsp->version());
                    }

                    auto setReq = ControllerServiceProxy->SetSpec();
                    setReq->set_expected_version(getRsp->version());
                    // Just random option.
                    setReq->set_spec(R"({"binary_version"="random_version";})");
                    auto setRsp = WaitFor(setReq->Invoke()).ValueOrThrow();
                    ASSERT_EQ(getRsp->version() + 1, setRsp->version());
                    targetVersion = setRsp->version();
                    return;
                } catch (const TErrorException& exception) {
                    if (exception.Error().FindMatching(NFlow::EErrorCode::PipelineStateVersionMismatch)) {
                        continue;
                    }
                    throw;
                }
            }

            auto getVersion = [state = PersistedStateManagerLocalState] {
                auto guard = Guard(state->Lock);
                return state->FlowView->State->ExecutionSpec->PipelineSpec->GetVersion().Underlying();
            };

            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
            while (getVersion() != targetVersion) {
                YT_LOG_INFO("Still wait until update of spec will be available in flow view.");
                NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::Seconds(1));
            }
        }

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, RecoverFlowCoreTargetIsReloadedOnEachIteration)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    auto recoverCallCount = std::make_shared<std::atomic<int>>(0);

    Prepare();

    EXPECT_CALL(*PersistedStateManager, RecoverFlowCoreTarget())
        .WillRepeatedly([recoverCallCount] {
            recoverCallCount->fetch_add(1);
            return New<TVersionedFlowCoreTarget>();
        });

    YT_LOG_INFO("Start RecoverFlowCoreTargetIsReloadedOnEachIteration");

    ExecuteViaControlQueue([&] {
        StartLeading();

        // Wait until RecoverFlowCoreTarget has been called at least 3 times:
        // 1 during initial recovery + at least 2 from the scheduler loop.
        while (true) {
            if (recoverCallCount->load() >= 3) {
                break;
            }
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, SetSpecFailsWhenFlowCoreTargetMismatches)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    EXPECT_CALL(*PersistedStateManager, RecoverFlowCoreTarget())
        .WillRepeatedly([] {
            auto target = New<TVersionedFlowCoreTarget>();
            target->SetValue(TFlowCoreTarget(std::string("mismatch_version")));
            return target;
        });

    YT_LOG_INFO("Start SetSpecFailsWhenFlowCoreTargetMismatches");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        while (true) {
            auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();
            if (!flowView->State->ExecutionSpec->FlowCoreTarget->GetValue().Underlying().empty()) {
                break;
            }
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }

        {
            auto getReq = ControllerServiceProxy->GetSpec();
            auto getRsp = WaitFor(getReq->Invoke()).ValueOrThrow();

            auto setReq = ControllerServiceProxy->SetSpec();
            setReq->set_expected_version(getRsp->version());
            setReq->set_spec(R"({"binary_version"="random_version";})");
            auto setResult = WaitFor(setReq->Invoke());

            ASSERT_FALSE(setResult.IsOK());
            EXPECT_THAT(ToString(setResult), ::testing::HasSubstr("Flow core target mismatches"));
            EXPECT_TRUE(setResult.FindMatching(NFlow::EErrorCode::FlowCoreTargetMismatch))
                << "SetSpec must report FlowCoreTargetMismatch on a value mismatch, got: " << ToString(setResult);
        }

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

// Verifies that a FlowCoreTargetMismatch detected by the SetPipelineSpecs
// pre-check fails fast.
TEST_F(TControllerTest, SetSpecDoesNotRetryOnFlowCoreTargetMismatch)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    YT_LOG_INFO("Start SetSpecDoesNotRetryOnFlowCoreTargetMismatch");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        // Build a dedicated FlowExecutor with its own PersistedStateManager mock so
        // that the scheduler thread's periodic RecoverFlowCoreTarget() calls don't
        // interfere with our call counter.
        auto executorPersistedStateManager = New<StrictMock<TMockPersistedStateManager>>();
        auto state = PersistedStateManagerLocalState;

        EXPECT_CALL(*executorPersistedStateManager, RecoverSpec())
            .WillRepeatedly([state] {
                auto guard = Guard(state->Lock);
                return CloneYsonStruct(state->Spec);
            });
        EXPECT_CALL(*executorPersistedStateManager, RecoverDynamicSpec())
            .WillRepeatedly([state] {
                auto guard = Guard(state->Lock);
                return CloneYsonStruct(state->DynamicSpec);
            });

        auto recoverFlowCoreTargetCallCount = std::make_shared<std::atomic<int>>(0);
        EXPECT_CALL(*executorPersistedStateManager, RecoverFlowCoreTarget())
            .WillRepeatedly([recoverFlowCoreTargetCallCount] {
                recoverFlowCoreTargetCallCount->fetch_add(1);
                auto target = New<TVersionedFlowCoreTarget>();
                target->SetValue(TFlowCoreTarget(std::string("mismatch_version")));
                return target;
            });

        // Use a config with multiple retries to make a regression (i.e. the
        // pre-check being retried) clearly observable.
        auto serviceConfig = New<TControllerServiceConfig>();
        serviceConfig->SetSpecRetryCount = 3;
        serviceConfig->SetSpecRetryPeriod = TDuration::MilliSeconds(1);

        auto flowExecutor = CreateFlowExecutor(
            Controller,
            executorPersistedStateManager,
            YTConnector,
            serviceConfig,
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            GetSyncInvoker());

        TSetPipelineSpecsArg arg;
        arg.Spec = ConvertToNode(TYsonStringBuf(TStringBuf(R"({"binary_version"="random_version";})")));

        // PersistSpecs must never be reached: the pre-check should throw before it.
        EXPECT_CALL(*executorPersistedStateManager, PersistSpecs(_, _, _, _))
            .Times(0);

        EXPECT_THROW_WITH_ERROR_CODE(
            flowExecutor->SetPipelineSpecs(arg),
            NFlow::EErrorCode::FlowCoreTargetMismatch);

        // The pre-check calls RecoverFlowCoreTarget exactly once per attempt.
        // With fail-fast, it must be called exactly once even though
        // SetSpecRetryCount is 3.
        EXPECT_EQ(recoverFlowCoreTargetCallCount->load(), 1)
            << "FlowCoreTargetMismatch must not be retried; "
               "RecoverFlowCoreTarget call count indicates the number of pre-check iterations";

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, WorkerExcludedFromSchedulingWhenFlowCoreTargetMismatches)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    // Set FlowCoreTarget to the controller's own binary checksum.
    PersistedStateManagerLocalState->FlowCoreTarget->SetValue(TFlowCoreTarget(GetBinaryChecksum()));

    Prepare();

    // Return two workers: one matching, one mismatching.
    TWorkerInfo matchingWorker;
    matchingWorker.RpcAddress = "matching-worker.net:81";
    matchingWorker.State = EWorkerState::Registered;
    matchingWorker.FlowCoreVersion = GetBinaryChecksum();
    matchingWorker.IncarnationId = TIncarnationId(TGuid::Create());

    TWorkerInfo mismatchingWorker;
    mismatchingWorker.RpcAddress = "mismatching-worker.net:81";
    mismatchingWorker.State = EWorkerState::Registered;
    mismatchingWorker.FlowCoreVersion = "old_version";
    mismatchingWorker.IncarnationId = TIncarnationId(TGuid::Create());

    EXPECT_CALL(*WorkerTracker, GetWorkers())
        .WillRepeatedly(Return(std::vector<TWorkerInfo>{matchingWorker, mismatchingWorker}));

    YT_LOG_INFO("Start WorkerExcludedFromSchedulingWhenFlowCoreTargetMismatches");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        // Wait until the flow view has workers populated.
        while (true) {
            auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();
            if (!flowView->State->Workers.empty()) {
                break;
            }
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }

        auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();

        // Only the matching worker should be in the Workers map.
        EXPECT_EQ(flowView->State->Workers.size(), 1u);
        EXPECT_TRUE(flowView->State->Workers.contains("matching-worker.net:81"));
        EXPECT_FALSE(flowView->State->Workers.contains("mismatching-worker.net:81"));

        // The mismatched worker should be in the ephemeral state, grouped by FlowCoreVersion.
        const auto& mismatchedGroups = flowView->EphemeralState->FlowCoreTargetMismatchedWorkers;
        EXPECT_EQ(mismatchedGroups.size(), 1u);
        auto it = mismatchedGroups.find("old_version");
        ASSERT_NE(it, mismatchedGroups.end());
        EXPECT_EQ(it->second.ExampleAddress, "mismatching-worker.net:81");
        EXPECT_EQ(it->second.Count, 1u);

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, WorkerIncludedWhenFlowCoreTargetEmpty)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    TWorkerInfo worker1;
    worker1.RpcAddress = "worker-1.net:81";
    worker1.State = EWorkerState::Registered;
    worker1.FlowCoreVersion = "any_version";
    worker1.IncarnationId = TIncarnationId(TGuid::Create());

    TWorkerInfo worker2;
    worker2.RpcAddress = "worker-2.net:81";
    worker2.State = EWorkerState::Registered;
    worker2.FlowCoreVersion = "another_version";
    worker2.IncarnationId = TIncarnationId(TGuid::Create());

    EXPECT_CALL(*WorkerTracker, GetWorkers())
        .WillRepeatedly(Return(std::vector<TWorkerInfo>{worker1, worker2}));

    YT_LOG_INFO("Start WorkerIncludedWhenFlowCoreTargetEmpty");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        while (true) {
            auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();
            if (flowView->State->Workers.size() >= 2) {
                break;
            }
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }

        auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();

        // Both workers should be included since FlowCoreTarget is empty.
        EXPECT_EQ(flowView->State->Workers.size(), 2u);
        EXPECT_TRUE(flowView->State->Workers.contains("worker-1.net:81"));
        EXPECT_TRUE(flowView->State->Workers.contains("worker-2.net:81"));
        EXPECT_TRUE(flowView->EphemeralState->FlowCoreTargetMismatchedWorkers.empty());

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, WorkerWithEmptyFlowCoreVersionExcludedWhenTargetSet)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    // Set FlowCoreTarget to a specific value.
    PersistedStateManagerLocalState->FlowCoreTarget->SetValue(TFlowCoreTarget(GetBinaryChecksum()));

    Prepare();

    // Worker without FlowCoreVersion (doesn't report it).
    TWorkerInfo oldWorker;
    oldWorker.RpcAddress = "old-worker.net:81";
    oldWorker.State = EWorkerState::Registered;
    oldWorker.FlowCoreVersion = "";
    oldWorker.IncarnationId = TIncarnationId(TGuid::Create());

    EXPECT_CALL(*WorkerTracker, GetWorkers())
        .WillRepeatedly(Return(std::vector<TWorkerInfo>{oldWorker}));

    YT_LOG_INFO("Start WorkerWithEmptyFlowCoreVersionExcludedWhenTargetSet");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        // Wait until at least one iteration has run with the worker present.
        while (true) {
            auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();
            if (!flowView->EphemeralState->FlowCoreTargetMismatchedWorkers.empty()) {
                break;
            }
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }

        auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();

        // Worker without FlowCoreVersion must be excluded when FlowCoreTarget is set.
        EXPECT_TRUE(flowView->State->Workers.empty());
        const auto& mismatchedGroups = flowView->EphemeralState->FlowCoreTargetMismatchedWorkers;
        EXPECT_EQ(mismatchedGroups.size(), 1u);
        auto it = mismatchedGroups.find("");
        ASSERT_NE(it, mismatchedGroups.end());
        EXPECT_EQ(it->second.ExampleAddress, "old-worker.net:81");
        EXPECT_EQ(it->second.Count, 1u);

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

struct TFlowCoreTargetMismatchTestParam
{
    EPipelineState PipelineState;
    EPipelineState TargetState;
};

class TFlowCoreTargetMismatchTest
    : public TControllerTest
    , public ::testing::WithParamInterface<TFlowCoreTargetMismatchTestParam>
{ };

// Simulates a controller restart where the persisted FlowCoreTarget does not
// match the running binary's FlowCoreVersion. The mismatch is seeded into the
// persistent state BEFORE leadership starts, so the very first scheduling
// iteration observes the mismatch and force-pauses the pipeline.
TEST_P(TFlowCoreTargetMismatchTest, PipelineIsPausedOnMismatch)
{
    const auto& param = GetParam();

    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    // Persist the initial PipelineState and TargetState before the controller starts leading.
    PersistedStateManagerLocalState->FlowView->State->ExecutionSpec
        ->PipelineState->SetValue(param.PipelineState);
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->TargetState = param.TargetState;
    PersistedStateManagerLocalState->DynamicSpec->SetValue(dynamicSpec);

    // Seed a FlowCoreTarget that does not match the controller's FlowCoreVersion.
    PersistedStateManagerLocalState->FlowCoreTarget
        ->SetValue(TFlowCoreTarget(std::string("mismatch_version")));

    YT_LOG_INFO("Start PipelineIsPausedOnMismatch (PipelineState: %v, TargetState: %v)",
        param.PipelineState,
        param.TargetState);

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        auto getPipelineState = [state = PersistedStateManagerLocalState] {
            auto guard = Guard(state->Lock);
            return state->FlowView->State->ExecutionSpec->PipelineState->GetValue();
        };

        while (getPipelineState() != EPipelineState::Paused) {
            YT_LOG_INFO("Waiting for pipeline to reach Paused after mismatch (CurrentState: %v)", getPipelineState());
            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
        }

        StopLeading();
    });
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TFlowCoreTargetMismatchTest,
    ::testing::Values(
        TFlowCoreTargetMismatchTestParam{
            .PipelineState = EPipelineState::Working,
            .TargetState = EPipelineState::Completed},
        TFlowCoreTargetMismatchTestParam{
            .PipelineState = EPipelineState::Draining,
            .TargetState = EPipelineState::Stopped}),
    [] (const auto& info) {
        return Format("%vTo%v", info.param.PipelineState, info.param.TargetState);
    });

////////////////////////////////////////////////////////////////////////////////

TEST(TControllerHelpersTest, SyncTraverseDataWithSpec)
{
    auto schema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});

    auto createComputationSpec = [&] (TStreamId inputStream, TStreamId outputStream) {
        auto spec = New<TComputationSpec>();
        spec->ComputationClassName = "::TTestComputation";
        spec->GroupBySchema = schema;
        if (!inputStream.Underlying().empty()) {
            spec->InputStreamIds.insert(inputStream);
        }
        if (!outputStream.Underlying().empty()) {
            spec->OutputStreamIds.insert(outputStream);
        }
        return spec;
    };

    auto createStreamSpec = [&] () {
        auto spec = New<TStreamSpec>();
        spec->Schema = schema;
        return spec;
    };

    auto createComputationDynamicSpec = [] {
        return ConvertTo<TDynamicComputationSpecPtr>(TYsonStringBuf(R"({"parameters" = {"desired_partition_count" = 1}})"));
    };

    auto spec = New<TPipelineSpec>();
    spec->Computations = {
        {TComputationId("reader"), createComputationSpec(TStreamId(), TStreamId("prepared"))},
        {TComputationId("reducer"), createComputationSpec(TStreamId("prepared"), TStreamId())},
    };
    spec->Streams = {
        {TStreamId("prepared"), createStreamSpec()},
    };

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->JobManager->AsyncBalancing = false;
    dynamicSpec->Computations = {
        {TComputationId("reader"), createComputationDynamicSpec()},
        {TComputationId("reducer"), createComputationDynamicSpec()},
    };


    auto flowView = New<TFlowView>();
    flowView->CurrentSpec->SetValue(spec);
    flowView->CurrentDynamicSpec->SetValue(dynamicSpec);
    flowView->State->ExecutionSpec->PipelineSpec->SetValue(spec);
    flowView->State->ExecutionSpec->ExtendedPipelineSpec->SetValue(BuildExtendedPipelineSpec(spec));
    flowView->State->ExecutionSpec->DynamicPipelineSpec->SetValue(dynamicSpec);
    flowView->State->CurrentTimestamp = TSystemTimestamp(10);

    SyncTraverseDataWithSpec(flowView);

    EXPECT_EQ(flowView->State->TraverseData->Streams.size(), 1u);
    ASSERT_EQ(GetOrCrash(flowView->State->TraverseData->Streams, TStreamId("prepared"))->SystemWatermark, TSystemTimestamp(10));
    // ASSERT_EQ(GetOrCrash(flowView->State->TraverseData->Computations, TComputationId("reader"))->SystemWatermark, TSystemTimestamp(10));

    // Update spec. Flow view must be recreated in this case in production but it is not required in test.
    spec->Computations = {
        {TComputationId("reader"), createComputationSpec(TStreamId(), TStreamId("raw"))},
        {TComputationId("mapper"), createComputationSpec(TStreamId("raw"), TStreamId("prepared"))},
        {TComputationId("reducer"), createComputationSpec(TStreamId("prepared"), TStreamId())},
    };
    spec->Streams = {
        {TStreamId("raw"), createStreamSpec()},
        {TStreamId("prepared"), createStreamSpec()},
    };

    flowView->State->ExecutionSpec->ExtendedPipelineSpec->SetValue(BuildExtendedPipelineSpec(spec));
    flowView->State->CurrentTimestamp = TSystemTimestamp(40);

    SyncTraverseDataWithSpec(flowView);

    EXPECT_EQ(flowView->State->TraverseData->Streams.size(), 2u);
    ASSERT_EQ(GetOrCrash(flowView->State->TraverseData->Streams, TStreamId("raw"))->SystemWatermark, TSystemTimestamp(40));
    ASSERT_EQ(GetOrCrash(flowView->State->TraverseData->Streams, TStreamId("prepared"))->SystemWatermark, TSystemTimestamp(10));
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, SetFlowCoreTargetFailsOnSpecVersionMismatch)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    auto persistCallCount = std::make_shared<std::atomic<int>>(0);
    EXPECT_CALL(*PersistedStateManager, PersistFlowCoreTarget(_, _))
        .WillRepeatedly([persistCallCount] (const TVersionedFlowCoreTargetPtr&, const TPipelineImportantVersionsPtr&) {
            persistCallCount->fetch_add(1);
            THROW_ERROR_EXCEPTION(NFlow::EErrorCode::SpecVersionMismatch,
                "Spec version mismatch (simulated)");
        });

    YT_LOG_INFO("Start SetFlowCoreTargetFailsOnSpecVersionMismatch");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        TSetFlowCoreTargetArg arg;
        arg.FlowCoreTarget = TFlowCoreTarget("v1.0.0");

        auto flowExecutor = CreateFlowExecutor(
            Controller,
            PersistedStateManager,
            YTConnector,
            New<TControllerServiceConfig>(),
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            GetSyncInvoker());

        EXPECT_THROW_WITH_ERROR_CODE(
            flowExecutor->SetFlowCoreTarget(arg),
            NFlow::EErrorCode::SpecVersionMismatch);
        EXPECT_EQ(persistCallCount->load(), 1);

        StopLeading();
    });
}

TEST_F(TControllerTest, SetFlowCoreTargetFailsOnFlowCoreTargetVersionMismatch)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    auto persistCallCount = std::make_shared<std::atomic<int>>(0);
    EXPECT_CALL(*PersistedStateManager, PersistFlowCoreTarget(_, _))
        .WillRepeatedly([persistCallCount] (const TVersionedFlowCoreTargetPtr&, const TPipelineImportantVersionsPtr&) {
            persistCallCount->fetch_add(1);
            THROW_ERROR_EXCEPTION(NFlow::EErrorCode::FlowCoreTargetVersionMismatch,
                "Flow core target mismatches");
        });

    YT_LOG_INFO("Start SetFlowCoreTargetFailsOnFlowCoreTargetVersionMismatch");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        TSetFlowCoreTargetArg arg;
        arg.FlowCoreTarget = TFlowCoreTarget("v1.0.0");

        auto flowExecutor = CreateFlowExecutor(
            Controller,
            PersistedStateManager,
            YTConnector,
            New<TControllerServiceConfig>(),
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            GetSyncInvoker());

        EXPECT_THROW_WITH_ERROR_CODE(
            flowExecutor->SetFlowCoreTarget(arg),
            NFlow::EErrorCode::FlowCoreTargetVersionMismatch);
        EXPECT_EQ(persistCallCount->load(), 1);

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, SetFlowCoreTargetIsIdempotentForSameValue)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    // Override the default Persist mock with a counting variant.
    auto persistCallCount = std::make_shared<std::atomic<int>>(0);
    EXPECT_CALL(*PersistedStateManager, PersistFlowCoreTarget(_, _))
        .WillRepeatedly(
            [persistCallCount, state = PersistedStateManagerLocalState] (
                const TVersionedFlowCoreTargetPtr& flowCoreTarget,
                const TPipelineImportantVersionsPtr& expectedVersions) {
                auto guard = Guard(state->Lock);
                state->CheckImportantVersions(expectedVersions);
                state->FlowCoreTarget = CloneYsonStruct(flowCoreTarget);
                persistCallCount->fetch_add(1);
            });

    YT_LOG_INFO("Start SetFlowCoreTargetIsIdempotentForSameValue");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        auto flowExecutor = CreateFlowExecutor(
            Controller,
            PersistedStateManager,
            YTConnector,
            New<TControllerServiceConfig>(),
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            GetSyncInvoker());

        TSetFlowCoreTargetArg arg;
        arg.FlowCoreTarget = TFlowCoreTarget(GetBinaryChecksum());
        arg.AllowUpdateOnPause = true;

        // First call: must persist and bump version.
        auto firstResult = flowExecutor->SetFlowCoreTarget(arg);
        EXPECT_EQ(persistCallCount->load(), 1);
        EXPECT_GT(firstResult.Version.Underlying(), 0);

        // Second call with the same value: must NOT persist (would otherwise
        // crash with YT_VERIFY in PersistFlowCoreTarget) and must return the
        // current version unchanged.
        auto secondResult = flowExecutor->SetFlowCoreTarget(arg);
        EXPECT_EQ(persistCallCount->load(), 1);
        EXPECT_EQ(secondResult.Version, firstResult.Version);

        // Third call with a different value: must persist again and bump.
        TSetFlowCoreTargetArg arg2;
        arg2.FlowCoreTarget = TFlowCoreTarget(std::string("some_other_checksum"));
        arg2.AllowUpdateOnPause = true;
        auto thirdResult = flowExecutor->SetFlowCoreTarget(arg2);
        EXPECT_EQ(persistCallCount->load(), 2);
        EXPECT_GT(thirdResult.Version.Underlying(), firstResult.Version.Underlying());

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, StartPipelineFailsOnFlowCoreTargetMismatch)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    EXPECT_CALL(*PersistedStateManager, RecoverFlowCoreTarget())
        .WillRepeatedly([] {
            auto target = New<TVersionedFlowCoreTarget>();
            target->SetValue(TFlowCoreTarget(std::string("mismatch_version")));
            return target;
        });

    YT_LOG_INFO("Start StartPipelineFailsOnFlowCoreTargetMismatch");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        auto flowExecutor = CreateFlowExecutor(
            Controller,
            PersistedStateManager,
            YTConnector,
            New<TControllerServiceConfig>(),
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            GetSyncInvoker());

        TSetTargetPipelineStateArg arg;
        arg.TargetPipelineState = EPipelineState::Completed;

        EXPECT_THROW_WITH_ERROR_CODE(
            flowExecutor->SetTargetPipelineState(arg),
            NFlow::EErrorCode::FlowCoreTargetMismatch);

        // Stopping/pausing must still work — they don't need a matching controller.
        TSetTargetPipelineStateArg stopArg;
        stopArg.TargetPipelineState = EPipelineState::Stopped;
        EXPECT_NO_THROW(flowExecutor->SetTargetPipelineState(stopArg));

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
