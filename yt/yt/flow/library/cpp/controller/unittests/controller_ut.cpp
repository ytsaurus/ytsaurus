#include <yt/yt/flow/library/cpp/common/unittests/mock/time_provider.h>
#include <yt/yt/flow/library/cpp/controller/controller.h>
#include <yt/yt/flow/library/cpp/controller/controller_service.h>
#include <yt/yt/flow/library/cpp/controller/flow_executor.h>
#include <yt/yt/flow/library/cpp/controller/flow_executor_runtime.h>
#include <yt/yt/flow/library/cpp/controller/state_manager.h>
#include <yt/yt/flow/library/cpp/controller/worker_tracker_service.h>

#include <yt/yt/flow/library/cpp/partitioning/partitioning_coordinator.h>

#include <yt/yt/flow/library/cpp/controller/config.h>

#include <yt/yt/flow/library/cpp/controller/unittests/mock/controller.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/persisted_state_manager.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/worker_tracker.h>
#include <yt/yt/flow/library/cpp/controller/unittests/mock/yt_connector.h>

#include <yt/yt/flow/library/cpp/common/checksum.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/unittests/mock/authenticator.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_batching_async_sink_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/sink_controller_base.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/flow/library/cpp/client/controller/controller_service_proxy.h>

#include <yt/yt/flow/library/cpp/common/controller/worker_tracker_service_proxy.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service.h>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <util/system/type_name.h>

namespace NYT::NFlow::NController {
namespace {

using namespace NApi;
using namespace NClient::NCache;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

using ::testing::_;
using ::testing::AnyNumber;
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

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, Logger, "TestLogger");

////////////////////////////////////////////////////////////////////////////////

std::atomic<i64> ControllerTestSinkChannelCount{5};

class TControllerTestSinkController
    : public TSinkControllerBase
{
public:
    using TSinkControllerBase::TSinkControllerBase;

    std::optional<i64> GetReceiverChannelCount() override
    {
        return ControllerTestSinkChannelCount.load();
    }
};

class TControllerTestSink
    : public TOrderedBatchingAsyncSinkBase
{
public:
    using TSinkController = TControllerTestSinkController;

    using TOrderedBatchingAsyncSinkBase::TOrderedBatchingAsyncSinkBase;

    void DoInit(const std::string& /*producerId*/) override
    { }

    TFuture<void> DoDistribute(const std::vector<TOutputMessageConstPtr>& /*messages*/, i64 /*seqNo*/) override
    {
        return OKFuture;
    }
};

YT_FLOW_DEFINE_SINK(TControllerTestSink);

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
    struct TPartitionLayoutSummary
    {
        int Executing = 0;
        int Interrupting = 0;
    };

    void CheckImportantVersions(const TPipelineImportantVersionsPtr& expectedVersions)
    {
        auto actual = MakePipelineImportantVersions(FlowView->State, Spec);
        actual->FlowCoreTargetVersion = FlowCoreTarget->GetVersion();
        expectedVersions->EnsureEqual(*actual);
    }

    void CheckDynamicImportantVersions(const TVersion& expectedVersion)
    {
        // Like the real manager, read only the version of the raw row.
        auto actualVersion = DynamicSpecNode ? ReadRawVersion(DynamicSpecNode) : DynamicSpec->GetVersion();

        THROW_ERROR_EXCEPTION_UNLESS(
            expectedVersion == actualVersion,
            NFlow::EErrorCode::SpecVersionMismatch,
            "Versions mismatch");
    }

    //! Resolves the state from the rows that the real persisted state manager would have written.
    TObservedPipelineState ObservePersistedPipelineState() const
    {
        auto importantVersions = MakePipelineImportantVersions(FlowView->State, Spec);
        importantVersions->FlowCoreTargetVersion = FlowCoreTarget->GetVersion();
        const THashMap<std::string, TYsonString> rows{
            {"important_versions", ConvertToYsonString(importantVersions)},
            {"pipeline_state_replica", ConvertToYsonString(Replica ? Replica : FlowView->State->ExecutionSpec->PipelineState)},
            {"flow_view", ConvertToYsonString(FlowView->State)},
        };
        return ResolvePipelineState([&] (const std::vector<std::string>& keys) {
            std::vector<std::optional<TYsonString>> values;
            for (const auto& key : keys) {
                values.push_back(GetOrCrash(rows, key));
            }

            return values;
        });
    }

    TSpinLock Lock;
    TTestDatabase Database;
    TIntrusivePtr<TStorageHandler> StorageHandler = New<TStorageHandler>(Database);
    TPersistedStateControlPtr<std::string> PersistedMasterControl = New<TPersistedStateControl<std::string>>(StorageHandler);
    TFlowViewPtr FlowView = New<TFlowView>();
    TVersionedPipelineSpecPtr Spec = New<TVersionedPipelineSpec>();
    TVersionedDynamicPipelineSpecPtr DynamicSpec = New<TVersionedDynamicPipelineSpec>();
    TVersionedFlowCoreTargetPtr FlowCoreTarget = New<TVersionedFlowCoreTarget>();
    //! Persisted replica; null means the one written with the current FlowView state.
    TVersionedPipelineStatePtr Replica;
    //! Raw canonical spec row that overrides #Spec until the next spec persist.
    INodePtr SpecNode;
    //! Raw canonical dynamic spec row that overrides #DynamicSpec until the next dynamic spec persist.
    INodePtr DynamicSpecNode;
    bool ObserveSinkTopologyRetry = false;
    bool SinkTopologyPersistenceFailureInjected = false;
    std::optional<TPartitionLayoutSummary> LastPersistedPartitionLayout;
    std::vector<TPartitionLayoutSummary> SinkTopologyPersistenceAttempts;
};

////////////////////////////////////////////////////////////////////////////////

class TControllerTest
    : public ::testing::Test
{
public:
    static constexpr ui64 InitialClockUnixTime = 1'000'000;

    //! Mints versions below the controller clock, so the controller can bump them.
    const IVersionProviderPtr TestStateVersionProvider = New<TFakeVersionProvider>(InitialClockUnixTime - 1);

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
                    TPersistedStateManagerLocalState::TPartitionLayoutSummary summary;
                    for (const auto& [_, partition] : flowState->ExecutionSpec->Layout->Partitions) {
                        if (partition->ComputationId != TComputationId("computation")) {
                            continue;
                        }

                        if (partition->State == EPartitionState::Executing) {
                            ++summary.Executing;
                        } else if (partition->State == EPartitionState::Interrupting) {
                            ++summary.Interrupting;
                        }
                    }
                    state->LastPersistedPartitionLayout = summary;
                    if (state->ObserveSinkTopologyRetry) {
                        if (state->SinkTopologyPersistenceFailureInjected || summary.Interrupting > 0) {
                            state->SinkTopologyPersistenceAttempts.push_back(summary);
                        }
                        if (!state->SinkTopologyPersistenceFailureInjected && summary.Interrupting > 0) {
                            state->SinkTopologyPersistenceFailureInjected = true;
                            THROW_ERROR_EXCEPTION(
                                NFlow::EErrorCode::SpecVersionMismatch,
                                "Injected flow state persistence failure");
                        }
                    }
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
                    }
                    if (dynamicSpec && expectedDynamicSpecVersion) {
                        state->CheckDynamicImportantVersions(*expectedDynamicSpecVersion);
                    }

                    if (spec && expectedVersions) {
                        state->Spec = CloneYsonStruct(*spec);
                        state->SpecNode.Reset();
                    }

                    if (dynamicSpec && expectedDynamicSpecVersion) {
                        state->DynamicSpec = CloneYsonStruct(*dynamicSpec);
                        state->DynamicSpecNode.Reset();
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
        EXPECT_CALL(*PersistedStateManager, RecoverSpecNode())
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] () {
                    auto guard = Guard(state->Lock);
                    return state->SpecNode ? state->SpecNode : ConvertToNode(state->Spec);
                });
        EXPECT_CALL(*PersistedStateManager, RecoverDynamicSpecNode())
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] () {
                    auto guard = Guard(state->Lock);
                    return state->DynamicSpecNode ? state->DynamicSpecNode : ConvertToNode(state->DynamicSpec);
                });
        EXPECT_CALL(*PersistedStateManager, RecoverPersistedPipelineState())
            .WillRepeatedly(
                [state = PersistedStateManagerLocalState] () {
                    auto guard = Guard(state->Lock);
                    return state->ObservePersistedPipelineState();
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
        Transaction = transaction;
        EXPECT_CALL(*transaction, SetNode(_, _, _))
            .WillRepeatedly(Return(OKFuture));
        EXPECT_CALL(*transaction, Commit(_))
            .WillRepeatedly(Return(MakeFuture(NApi::TTransactionCommitResult{})));
        EXPECT_CALL(*YTConnector, StartTransaction(_, _))
            .WillRepeatedly(Return(MakeFuture<NApi::ITransactionPtr>(transaction)));
        auto client = New<StrictMock<NApi::TMockClient>>();
        auto timestampProvider = New<StrictMock<NTransactionClient::TMockTimestampProvider>>();
        auto currentTimestamp = std::make_shared<std::atomic<ui64>>(
            NTransactionClient::TimestampFromUnixTime(InitialClockUnixTime).Underlying());
        EXPECT_CALL(*timestampProvider, GenerateTimestamps(_, _))
            .WillRepeatedly([currentTimestamp] (int count, NObjectClient::TCellTag /*clockClusterTag*/) {
                return MakeFuture<NTransactionClient::TTimestamp>(NTransactionClient::TTimestamp(
                    currentTimestamp->fetch_add(
                        static_cast<ui64>(count) << NTransactionClient::TimestampCounterWidth)));
            });
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
        // Leadership published long ago, so the warm-up never holds job management back; the tests
        // that care about the warm-up override this.
        EXPECT_CALL(*YTConnector, GetLeadershipPublishTime())
            .WillRepeatedly(Return(TInstant::Now() - TDuration::Hours(1)));
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
                YT_TLOG_INFO("Still wait until controller starts leading")
                    .With(ex);
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

        // Every committed scheduling iteration reports the end of leader recovery, carrying the
        // epoch it read when it started leading; the fixture is strict, so both calls need an
        // expectation even though no test asserts on them.
        EXPECT_CALL(*YTConnector, GetLeadershipEpoch())
            .WillRepeatedly(Return(0));
        EXPECT_CALL(*YTConnector, OnLeaderRecoveryFinished(_))
            .Times(AnyNumber());

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
            /*httpClient*/ nullptr,
            /*ignoreSingletonsDynamicConfig*/ false,
            /*clockClusterTag*/ NObjectClient::InvalidCellTag,
            RootStatusProfiler);

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
            authenticator->CreateYTControllerRpcAuthenticator(),
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

    //! Pass #RootStatusProfiler to see the controller errors, as production does.
    IFlowExecutorPtr CreateTestFlowExecutor(IStatusProfilerPtr statusProfiler = CreateSyncStatusProfiler())
    {
        return CreateFlowExecutor(
            Controller,
            PersistedStateManager,
            YTConnector,
            New<TControllerServiceConfig>(),
            /*orchidRoot*/ nullptr,
            std::move(statusProfiler),
            GetSyncInvoker());
    }

    //! Starts leading with a persisted FlowState that cannot be recovered, so the FlowView keeper stays uninitialized.
    void StartLeadingWithoutFlowView()
    {
        EXPECT_CALL(*PersistedStateManager, RecoverFlowState())
            .WillRepeatedly([] () -> TFlowStatePtr {
                THROW_ERROR_EXCEPTION("Injected unrecoverable FlowState");
            });
        StartLeading();
    }

    void SetPersistedPipelineState(EPipelineState state)
    {
        auto guard = Guard(PersistedStateManagerLocalState->Lock);
        PersistedStateManagerLocalState->FlowView->State->ExecutionSpec->PipelineState->TrySetValue(state, TestStateVersionProvider);
    }

    //! Sets the canonical spec and the one embedded in the persisted FlowState.
    void SeedPersistedSpec(TStringBuf specYson)
    {
        const auto& state = PersistedStateManagerLocalState;
        auto guard = Guard(state->Lock);
        state->Spec->TrySetValue(ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(specYson)), TestStateVersionProvider);
        state->FlowView->State->ExecutionSpec->PipelineSpec = CloneYsonStruct(state->Spec);
    }

    //! Sets the canonical dynamic spec and the one embedded in the persisted FlowState.
    void SeedPersistedDynamicSpec(TStringBuf dynamicSpecYson)
    {
        const auto& state = PersistedStateManagerLocalState;
        auto guard = Guard(state->Lock);
        state->DynamicSpec->TrySetValue(ConvertTo<TDynamicPipelineSpecPtr>(TYsonStringBuf(dynamicSpecYson)), TestStateVersionProvider);
        state->FlowView->State->ExecutionSpec->DynamicPipelineSpec = CloneYsonStruct(state->DynamicSpec);
    }

    EPipelineState GetPersistedPipelineState() const
    {
        auto guard = Guard(PersistedStateManagerLocalState->Lock);
        return PersistedStateManagerLocalState->FlowView->State->ExecutionSpec->PipelineState->GetValue();
    }

    //! Polls |predicate| from a fiber until it holds or |timeout| expires; returns its last value.
    static bool WaitForCondition(const std::function<bool()>& predicate, TDuration timeout = TDuration::Seconds(30))
    {
        const auto deadline = TInstant::Now() + timeout;
        while (!predicate() && TInstant::Now() < deadline) {
            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
        }

        return predicate();
    }

public:
    TControllerConfigPtr ControllerConfig;
    TMockPersistedStateManagerPtr PersistedStateManager;
    TMockWorkerTrackerPtr WorkerTracker;
    TMockYTConnectorPtr YTConnector;
    //! The transaction every mocked start and attach hands out; job leases are built on it.
    TIntrusivePtr<StrictMock<NApi::TMockTransaction>> Transaction;
    TControlActionQueuePtr ControlActionQueue;
    NConcurrency::IFairShareThreadPoolPtr ControllerThreadPool;

    const IStatusProfilerPtr RootStatusProfiler = CreateSyncStatusProfiler();
    IControllerPtr Controller;
    TCallback<void()> StartLeading;
    TCallback<void()> StopLeading;

    TIntrusivePtr<TPersistedStateManagerLocalState> PersistedStateManagerLocalState;

    NRpc::IServerPtr LocalServer;
    std::unique_ptr<TControllerServiceProxy> ControllerServiceProxy;
};

////////////////////////////////////////////////////////////////////////////////

//! Adds a source computation producing |streamId|, so that the spec passes validation.
void AddStreamProducer(const TPipelineSpecPtr& spec, const TStreamId& streamId)
{
    auto producer = New<TComputationSpec>();
    producer->ComputationClassName = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
    producer->OutputStreamIds = {streamId};
    producer->SourceStreams["source"] = New<TSourceSpec>();
    producer->SourceStreams["source"]->SourceClassName = "NYT::NFlow::TRandomSource";
    producer->Postprocess();
    spec->Computations["producer"] = producer;
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, FailedPersistenceRetriesSinkTopologyRecreation)
{
    ControllerConfig->WarmUpTime = TDuration::Zero();
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);
    ControllerTestSinkChannelCount = 5;

    auto schema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});
    auto spec = New<TPipelineSpec>();
    auto computationSpec = New<TComputationSpec>();
    computationSpec->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
    computationSpec->GroupBySchema = schema;
    computationSpec->InputStreamIds.insert("input_stream");
    computationSpec->OutputStreamIds.insert("output_stream");
    computationSpec->Sinks["sink"] = New<TSinkSpec>();
    computationSpec->Sinks["sink"]->SinkClassName = TypeName<TControllerTestSink>();
    spec->Computations["computation"] = computationSpec;
    for (const auto& streamId : {TStreamId("input_stream"), TStreamId("output_stream")}) {
        spec->Streams[streamId] = New<TStreamSpec>();
        spec->Streams[streamId]->Schema = schema;
    }

    AddStreamProducer(spec, "input_stream");

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->TargetState = EPipelineState::Working;
    dynamicSpec->JobManager->AsyncBalancing = false;
    dynamicSpec->Computations["computation"] = New<TDynamicComputationSpec>();
    dynamicSpec->Computations["computation"]->Parameters->AddChild(
        "desired_partition_count",
        ConvertToNode(10));

    auto versionProvider = New<TFakeVersionProvider>(1);
    const auto& state = PersistedStateManagerLocalState;
    state->FlowView->State->AttachToControl(state->PersistedMasterControl);
    state->PersistedMasterControl->Recover();
    state->Spec->TrySetValue(spec, versionProvider);
    state->DynamicSpec->TrySetValue(dynamicSpec, versionProvider);
    state->FlowView->State->ExecutionSpec->PipelineSpec = CloneYsonStruct(state->Spec);
    state->FlowView->State->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(
        BuildExtendedPipelineSpec(spec),
        versionProvider);
    state->FlowView->State->ExecutionSpec->DynamicPipelineSpec = CloneYsonStruct(state->DynamicSpec);
    state->FlowView->State->ExecutionSpec->PipelineState->TrySetValue(
        EPipelineState::Working,
        versionProvider);

    Prepare();

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        auto waitFor = [&] (auto&& predicate) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(60);
            while (!predicate() && TInstant::Now() < deadline) {
                TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
            }
            return predicate();
        };

        if (!waitFor([&] {
                auto guard = Guard(state->Lock);
                return state->LastPersistedPartitionLayout &&
                    state->LastPersistedPartitionLayout->Executing == 10 &&
                    state->LastPersistedPartitionLayout->Interrupting == 0;
            })) {
            StopLeading();
            FAIL() << "Initial partition layout was not persisted";
        }

        {
            auto guard = Guard(state->Lock);
            state->ObserveSinkTopologyRetry = true;
        }
        ControllerTestSinkChannelCount = 7;

        if (!waitFor([&] {
                auto guard = Guard(state->Lock);
                return state->SinkTopologyPersistenceAttempts.size() >= 2;
            })) {
            StopLeading();
            FAIL() << "Sink topology persistence retry was not observed";
        }

        StopLeading();

        auto guard = Guard(state->Lock);
        ASSERT_GE(state->SinkTopologyPersistenceAttempts.size(), 2u);
        for (int index = 0; index < 2; ++index) {
            EXPECT_EQ(state->SinkTopologyPersistenceAttempts[index].Executing, 10);
            EXPECT_EQ(state->SinkTopologyPersistenceAttempts[index].Interrupting, 10);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, KeyCountLatestStatusSurvivesPersistenceRetry)
{
    ControllerConfig->WarmUpTime = TDuration::Zero();
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);
    ControllerConfig->FeedbackPeriod = TDuration::MilliSeconds(10);
    auto schema = New<NTableClient::TTableSchema>(std::vector{
        NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true),
    });
    auto spec = New<TPipelineSpec>();
    auto computation = New<TComputationSpec>();
    computation->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
    computation->GroupBySchema = schema;
    computation->InputStreamIds.insert("input");
    computation->OutputStreamIds.insert("output");
    spec->Computations["computation"] = computation;
    for (const auto& streamId : {TStreamId("input"), TStreamId("output")}) {
        spec->Streams[streamId] = New<TStreamSpec>();
        spec->Streams[streamId]->Schema = schema;
    }

    AddStreamProducer(spec, "input");
    auto dynamic = New<TDynamicPipelineSpec>();
    dynamic->TargetState = EPipelineState::Working;
    dynamic->JobManager->AsyncBalancing = false;
    dynamic->Computations["computation"] = New<TDynamicComputationSpec>();
    computation->Parameters = ConvertTo<IMapNodePtr>(TYsonStringBuf("{key_count_partitioning={enable=%true;};}"));
    const auto& local = PersistedStateManagerLocalState;
    const auto& state = local->FlowView->State;
    const auto versions = New<TFakeVersionProvider>(1);
    state->AttachToControl(local->PersistedMasterControl);
    local->PersistedMasterControl->Recover();
    local->Spec->TrySetValue(spec, versions);
    local->DynamicSpec->TrySetValue(dynamic, versions);
    state->ExecutionSpec->PipelineSpec = CloneYsonStruct(local->Spec);
    state->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(BuildExtendedPipelineSpec(spec), versions);
    state->ExecutionSpec->DynamicPipelineSpec = CloneYsonStruct(local->DynamicSpec);
    state->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, versions);
    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("computation");
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = 1;
    partition->StateTimestamp = TInstant::Now();
    partition->LowerKey = MinKey();
    partition->UpperKey = MaxKey();
    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->PartitionId = partition->PartitionId;
    job->WorkerAddress = "worker";
    job->WorkerIncarnationId = TIncarnationId(TGuid::Create());
    state->StartMutation();
    state->ExecutionSpec->Layout->CreatePartition(partition);
    state->ExecutionSpec->Layout->CreateJob(job);
    state->CommitMutation();
    Prepare();
    EXPECT_CALL(*YTConnector, GetLeadershipPublishTime())
        .WillRepeatedly(Return(TInstant::Zero()));
    EXPECT_CALL(*Transaction, GetId())
        .WillRepeatedly(Return(TGuid::Create()));
    auto aborted = std::make_shared<ITransaction::TAbortedHandler>();
    EXPECT_CALL(*Transaction, SubscribeAborted(_))
        .WillRepeatedly(SaveArg<0>(aborted.get()));
    EXPECT_CALL(*Transaction, Abort(_))
        .WillRepeatedly([aborted] (const TTransactionAbortOptions&) {
            (*aborted)(TError("Lease aborted"));
            return OKFuture;
        });
    auto failNext = std::make_shared<std::atomic<bool>>(false);
    const auto failed = NewPromise<void>();
    const auto retry = NewPromise<void>();
    EXPECT_CALL(*PersistedStateManager, PersistFlowState(_, _))
        .WillRepeatedly([local, failNext, failed, retry] (const TFlowStatePtr& flowState, const TPipelineImportantVersionsPtr& expected) {
            if (failNext->exchange(false)) {
                failed.Set();
                WaitFor(retry.ToFuture()).ThrowOnError();
                THROW_ERROR_EXCEPTION(NFlow::EErrorCode::SpecVersionMismatch, "Injected persistence failure");
            }
            auto guard = Guard(local->Lock);
            flowState->CommitMutation();
            local->CheckImportantVersions(expected);
            local->FlowView->State = CloneYsonStruct(flowState);
        });
    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();
        auto stop = Finally([&] {
            retry.TrySet();
            StopLeading();
        });
        auto waitFor = [&] (auto predicate) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(10);
            while (!predicate()) {
                if (TInstant::Now() >= deadline) {
                    return false;
                }
                TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
            }
            return true;
        };
        failNext->store(true);
        ASSERT_TRUE(waitFor([&] {
            return failed.IsSet();
        }));
        const auto observedAt = TInstant::Now();
        const THashMap<TKey, TInstant> keys{{MakeKey(17u), observedAt}, {MakeKey(18u), observedAt}};
        auto report = New<TJobStatus>();
        report->JobId = job->JobId;
        report->Epoch = 1;
        report->UpdateTime = observedAt;
        report->ObservedKeys = THashMap<TKey, TInstant>{{MakeKey(17u), observedAt}};
        Controller->RegisterJobStatus(job->JobId, report);
        auto heartbeat = CloneYsonStruct(report);
        heartbeat->UpdateTime += TDuration::MicroSeconds(1);
        Controller->RegisterJobStatus(job->JobId, heartbeat);
        auto waitForHeartbeat = [&] {
            return waitFor([&] {
                auto view = Controller->GetFlowViewKeeper()->GetFlowView();
                const auto* feedback = view->Feedback->PartitionJobStatuses.FindPtr(partition->PartitionId);
                return feedback && (*feedback)->CurrentJobStatus &&
                    (*feedback)->CurrentJobStatus->UpdateTime == heartbeat->UpdateTime &&
                    (*feedback)->CurrentJobStatus->ObservedKeys == heartbeat->ObservedKeys;
            });
        };
        ASSERT_TRUE(waitForHeartbeat());
        heartbeat = CloneYsonStruct(heartbeat);
        heartbeat->UpdateTime += TDuration::MicroSeconds(1);
        Controller->RegisterJobStatus(job->JobId, heartbeat);
        ASSERT_TRUE(waitForHeartbeat());

        auto stale = New<TJobStatus>();
        stale->JobId = TJobId(TGuid::Create());
        stale->ObservedKeys = THashMap<TKey, TInstant>{{MakeKey(99u), observedAt}};
        Controller->RegisterJobStatus(stale->JobId, stale);

        auto terminal = New<TJobStatus>();
        terminal->JobId = job->JobId;
        terminal->IsFinished = true;
        terminal->Epoch = 1;
        terminal->UpdateTime = TInstant::Now();
        terminal->ObservedKeys = keys;
        Controller->RegisterJobStatus(job->JobId, terminal);
        ASSERT_TRUE(waitFor([&] {
            auto view = Controller->GetFlowViewKeeper()->GetFlowView();
            const auto* feedback = view->Feedback->PartitionJobStatuses.FindPtr(partition->PartitionId);
            return feedback && (*feedback)->CurrentJobStatus && (*feedback)->CurrentJobStatus->IsFinished;
        }));
        retry.Set();
        ASSERT_TRUE(waitFor([&] {
            auto guard = Guard(local->Lock);
            const auto& persisted = local->FlowView->State;
            const auto* states = persisted->JobManagerState->Computations.FindPtr(TComputationId(TStateManager::PartitioningStateComputationId));
            const auto* value = states ? states->FindPtr("/v1") : nullptr;
            auto view = Controller->GetFlowViewKeeper()->GetFlowView();
            const auto* currentPartition = view->State->ExecutionSpec->Layout->Partitions.FindPtr(partition->PartitionId);
            if (!value || !currentPartition || (*currentPartition)->CurrentJobId) {
                return false;
            }
            const auto saved = ConvertTo<NPartitioning::TPartitioningCoordinatorStatePtr>(*value);
            return GetOrCrash(saved->Computations, TComputationId("computation"))->KeyCountPartitioning->ObservedKeys == keys;
        }));
    });
}

TEST_F(TControllerTest, StaleJobStatusIsIgnoredAfterReassignment)
{
    ControllerConfig->WarmUpTime = TDuration::Hours(1);
    ControllerConfig->FeedbackPeriod = TDuration::MilliSeconds(10);

    const auto partitionId = TPartitionId(TGuid::Create());
    const auto oldJobId = TJobId(TGuid::Create());
    const auto replacementJobId = TJobId(TGuid::Create());
    const auto initialStateTimestamp = TInstant::Now();

    const auto& state = PersistedStateManagerLocalState->FlowView->State;
    state->AttachToControl(PersistedStateManagerLocalState->PersistedMasterControl);
    PersistedStateManagerLocalState->PersistedMasterControl->Recover();

    state->StartMutation();
    auto partition = New<TPartition>();
    partition->PartitionId = partitionId;
    partition->ComputationId = TComputationId("computation");
    partition->State = EPartitionState::Executing;
    partition->StateEpoch = 7;
    partition->StateTimestamp = initialStateTimestamp;
    state->ExecutionSpec->Layout->CreatePartition(partition);

    auto oldJob = New<TJob>();
    oldJob->JobId = oldJobId;
    oldJob->PartitionId = partitionId;
    oldJob->WorkerAddress = "old-worker";
    oldJob->WorkerIncarnationId = TIncarnationId(TGuid::Create());
    state->ExecutionSpec->Layout->CreateJob(oldJob);
    state->CommitMutation();

    state->StartMutation();
    state->ExecutionSpec->Layout->RemoveJob(oldJobId, EJobFinishReason::LostWorker);
    auto replacementJob = New<TJob>();
    replacementJob->JobId = replacementJobId;
    replacementJob->PartitionId = partitionId;
    replacementJob->WorkerAddress = "replacement-worker";
    replacementJob->WorkerIncarnationId = TIncarnationId(TGuid::Create());
    state->ExecutionSpec->Layout->CreateJob(replacementJob);
    state->CommitMutation();

    Prepare();

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        auto waitForFeedback = [&] (auto&& predicate) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(5);
            while (!predicate()) {
                ASSERT_LT(TInstant::Now(), deadline);
                TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
            }
        };

        waitForFeedback([&] {
            // Hold the view: the controller replaces it on every feedback collection.
            auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();
            const auto* status = flowView->Feedback->PartitionJobStatuses.FindPtr(partitionId);
            return status && (*status)->CurrentJobId == replacementJobId;
        });

        const auto feedbackUpdateTime = Controller->GetFlowViewKeeper()->GetFlowView()->Feedback->UpdateTime;
        auto staleStatus = New<TJobStatus>();
        staleStatus->JobId = oldJobId;
        staleStatus->IsFinished = true;
        staleStatus->ObservedKeys = THashMap<TKey, TInstant>{{MakeKey(1u), TInstant::Now()}};
        staleStatus->Epoch = 100;
        staleStatus->UpdateTime = TInstant::Now();
        Controller->RegisterJobStatus(oldJobId, staleStatus);

        waitForFeedback([&] {
            auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();
            return flowView->Feedback->UpdateTime > feedbackUpdateTime;
        });

        const auto& flowView = Controller->GetFlowViewKeeper()->GetFlowView();
        const auto& jobStatus = GetOrCrash(flowView->Feedback->PartitionJobStatuses, partitionId);
        EXPECT_EQ(jobStatus->CurrentJobId, replacementJobId);
        EXPECT_FALSE(jobStatus->CurrentJobStatus);

        const auto& currentPartition = GetOrCrash(flowView->State->ExecutionSpec->Layout->Partitions, partitionId);
        EXPECT_EQ(currentPartition->CurrentJobId, replacementJobId);
        EXPECT_EQ(currentPartition->State, EPartitionState::Executing);
        EXPECT_EQ(currentPartition->StateEpoch, 7);
        EXPECT_EQ(currentPartition->StateTimestamp, initialStateTimestamp);
        EXPECT_TRUE(flowView->State->ExecutionSpec->Layout->Jobs.contains(replacementJobId));
        EXPECT_FALSE(flowView->State->ExecutionSpec->Layout->Jobs.contains(oldJobId));

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, WaitPersist)
{
    Prepare();
    YT_TLOG_INFO("Start");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        {
            auto setDynamicSpec = [&] (TStringBuf spec, i64 expectedVersion) {
                TSetPipelineDynamicSpecArg argument;
                argument.Spec = ConvertTo<INodePtr>(TYsonStringBuf(spec));
                argument.ExpectedVersion = TVersion(expectedVersion);

                auto setReq = ControllerServiceProxy->FlowExecute();
                setReq->set_command("set-pipeline-dynamic-spec");
                setReq->set_argument(ConvertToYsonString(argument).ToString());
                auto setRsp = WaitFor(setReq->Invoke()).ValueOrThrow();
                return ConvertTo<TSetPipelineDynamicSpecResult>(TYsonStringBuf(setRsp->result())).Version.Underlying();
            };

            auto getReq = ControllerServiceProxy->GetDynamicSpec();
            auto getRsp = WaitFor(getReq->Invoke()).ValueOrThrow();

            // Just random option.
            auto setVersion = setDynamicSpec(
                R"({"controller_connector"={"controller_wait_timeout"=1234;};})",
                getRsp->version());
            ASSERT_GT(setVersion, getRsp->version());

            auto getDynamicVersion = [state = PersistedStateManagerLocalState] {
                auto guard = Guard(state->Lock);
                return state->FlowView->State->ExecutionSpec->DynamicPipelineSpec->GetVersion().Underlying();
            };

            NConcurrency::TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(100));
            while (getDynamicVersion() != setVersion) {
                YT_TLOG_INFO("Still wait until update of dynamic spec will be available in flow view.");
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
                    ASSERT_GT(setRsp->version(), getRsp->version());
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
                YT_TLOG_INFO("Still wait until update of spec will be available in flow view.");
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

    YT_TLOG_INFO("Start RecoverFlowCoreTargetIsReloadedOnEachIteration");

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
            target->TrySetValue(TFlowCoreTarget(std::string("mismatch_version")), TestVersionProvider());
            return target;
        });

    YT_TLOG_INFO("Start SetSpecFailsWhenFlowCoreTargetMismatches");

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

    YT_TLOG_INFO("Start SetSpecDoesNotRetryOnFlowCoreTargetMismatch");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        // Build a dedicated FlowExecutor with its own PersistedStateManager mock so
        // that the scheduler thread's periodic RecoverFlowCoreTarget() calls don't
        // interfere with our call counter.
        auto executorPersistedStateManager = New<StrictMock<TMockPersistedStateManager>>();
        auto state = PersistedStateManagerLocalState;

        EXPECT_CALL(*executorPersistedStateManager, RecoverSpecNode())
            .WillRepeatedly([state] {
                auto guard = Guard(state->Lock);
                return ConvertToNode(state->Spec);
            });
        EXPECT_CALL(*executorPersistedStateManager, RecoverDynamicSpecNode())
            .WillRepeatedly([state] {
                auto guard = Guard(state->Lock);
                return ConvertToNode(state->DynamicSpec);
            });

        auto recoverFlowCoreTargetCallCount = std::make_shared<std::atomic<int>>(0);
        EXPECT_CALL(*executorPersistedStateManager, RecoverFlowCoreTarget())
            .WillRepeatedly([recoverFlowCoreTargetCallCount] {
                recoverFlowCoreTargetCallCount->fetch_add(1);
                auto target = New<TVersionedFlowCoreTarget>();
                target->TrySetValue(TFlowCoreTarget(std::string("mismatch_version")), TestVersionProvider());
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

// A spec write lands in persisted state and the keeper's CurrentSpec, but the controller
// ingests it into ExecutionSpec only on a scheduler iteration; until then the flow view is
// desynced and the true pipeline state is unobservable. A new spec must be rejected in this
// window, while an identical retry (e.g. the runner re-sending after a lost ack) must stay
// a successful no-op.
TEST_F(TControllerTest, SetSpecOnDesyncedFlowView)
{
    // Keep the flow view permanently desynced after the first spec write: the keeper is
    // initialized before the warm-up sleep, so no scheduler iteration ever ingests specs.
    ControllerConfig->WarmUpTime = TDuration::Hours(1);

    Prepare();

    YT_TLOG_INFO("Start SetSpecOnDesyncedFlowView");

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        ASSERT_TRUE(Controller->GetFlowViewKeeper()->GetFlowView()->IsSynced());

        constexpr TStringBuf firstSpec = R"({"binary_version"="first_version";})";

        i64 firstVersion = -1;
        {
            // First-time spec set: the view is synced and the pipeline state is Unknown.
            auto setReq = ControllerServiceProxy->SetSpec();
            setReq->set_spec(std::string(firstSpec));
            auto setRsp = WaitFor(setReq->Invoke()).ValueOrThrow();
            firstVersion = setRsp->version();
        }

        ASSERT_FALSE(Controller->GetFlowViewKeeper()->GetFlowView()->IsSynced());

        {
            // Identical retry: short-circuits on no version change before the gate.
            auto setReq = ControllerServiceProxy->SetSpec();
            setReq->set_expected_version(firstVersion);
            setReq->set_spec(std::string(firstSpec));
            auto setRsp = WaitFor(setReq->Invoke()).ValueOrThrow();
            ASSERT_EQ(setRsp->version(), firstVersion);
        }

        {
            // A different spec must not pass while the controller has not ingested the
            // previous one: the true pipeline state is unknown and may be Working.
            auto setReq = ControllerServiceProxy->SetSpec();
            setReq->set_expected_version(firstVersion);
            setReq->set_spec(R"({"binary_version"="second_version";})");
            auto setResult = WaitFor(setReq->Invoke());

            ASSERT_FALSE(setResult.IsOK());
            EXPECT_THAT(ToString(setResult), ::testing::HasSubstr("FlowView is not synced"));
        }

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TSetPipelineSpecsArg MakeSetPipelineSpecsArg(std::optional<TStringBuf> binaryVersion, std::optional<i64> controllerWaitTimeout)
{
    TSetPipelineSpecsArg argument;
    if (binaryVersion) {
        argument.Spec = ConvertToNode(TYsonStringBuf(Format("{binary_version=%Qv;}", *binaryVersion)));
    }

    if (controllerWaitTimeout) {
        auto dynamicSpec = Format("{controller_connector={controller_wait_timeout=%v;};}", *controllerWaitTimeout);
        argument.DynamicSpec = ConvertToNode(TYsonStringBuf(dynamicSpec));
    }

    return argument;
}

TYsonString ExecuteCommand(const IFlowExecutorPtr& executor, const std::string& command, TStringBuf argument)
{
    return executor->Execute(command, TYsonString(TYsonStringBuf(argument)), "root");
}

////////////////////////////////////////////////////////////////////////////////

struct TPipelineStateGateCase
{
    std::string Name;
    std::optional<TStringBuf> ImportantVersions;
    std::optional<TStringBuf> Replica;
    std::optional<TStringBuf> FlowView;

    //! Unset if the state cannot be resolved.
    std::optional<EPipelineState> State;
    bool StaticUpdateAllowed = false;
    bool StaticUpdateAllowedWithForce = false;
    bool DynamicOnlyUpdateAllowed = false;
    bool FlowCoreTargetUpdateAllowed = false;
    bool ReadsFlowView = true;
    //! FlowState does not match the important state version, which the single writer never persists.
    bool StateVersionMismatch = false;
};

// Checks which state the persisted-path gates and get-pipeline-state see and what the gates allow
// for given persisted rows of one snapshot. Without a FlowView, spec sync is not checked.
TEST(TPipelineStateGateContractTest, PipelineStateGateContract)
{
    constexpr TStringBuf ImportantVersions = "{pipeline_spec_versions=10;pipeline_state_versions=5;flow_core_target_version=0;}";
    const std::vector<TPipelineStateGateCase> cases = {
        {
            .Name = "FreshReplica",
            .ImportantVersions = ImportantVersions,
            .Replica = "{version=5;value=stopped;}",
            .FlowView = "{execution_spec={pipeline_state={version=5;value=stopped;};pipeline_spec={version=10;};};}",
            .State = EPipelineState::Stopped,
            .StaticUpdateAllowed = true,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
            .FlowCoreTargetUpdateAllowed = true,
            .ReadsFlowView = false,
        },
        {
            .Name = "AppliedSpecLagsCanonical",
            .ImportantVersions = ImportantVersions,
            .Replica = "{version=5;value=stopped;}",
            .FlowView = "{execution_spec={pipeline_state={version=5;value=stopped;};pipeline_spec={version=9;};};}",
            .State = EPipelineState::Stopped,
            .StaticUpdateAllowed = true,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
            .FlowCoreTargetUpdateAllowed = true,
            .ReadsFlowView = false,
        },
        {
            .Name = "ReplicaStaleAfterRollback",
            .ImportantVersions = "{pipeline_spec_versions=10;pipeline_state_versions=7;flow_core_target_version=0;}",
            .Replica = "{version=5;value=stopped;}",
            .FlowView = "{execution_spec={pipeline_state={version=7;value=working;};};}",
            .State = EPipelineState::Working,
            .DynamicOnlyUpdateAllowed = true,
        },
        {
            .Name = "MissingReplica",
            .ImportantVersions = "{pipeline_spec_versions=10;pipeline_state_versions=3;flow_core_target_version=0;}",
            .FlowView = "{execution_spec={pipeline_state={version=3;value=paused;};};}",
            .State = EPipelineState::Paused,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
        },
        {
            .Name = "GarbageReplica",
            .ImportantVersions = ImportantVersions,
            .Replica = "garbage",
            .FlowView = "{execution_spec={pipeline_state={version=5;value=stopped;};};}",
            .State = EPipelineState::Stopped,
            .StaticUpdateAllowed = true,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
            .FlowCoreTargetUpdateAllowed = true,
        },
        {
            .Name = "ReplicaInPreviousFormat",
            .ImportantVersions = ImportantVersions,
            .Replica = "{pipeline_state={version=5;value=working;};pipeline_spec_version=10;dynamic_pipeline_spec_version=20;}",
            .FlowView = "{execution_spec={pipeline_state={version=5;value=stopped;};};}",
            .State = EPipelineState::Stopped,
            .StaticUpdateAllowed = true,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
            .FlowCoreTargetUpdateAllowed = true,
        },
        {
            .Name = "ReplicaNewerThanImportantVersions",
            .ImportantVersions = ImportantVersions,
            .Replica = "{version=7;value=working;}",
            .FlowView = "{execution_spec={pipeline_state={version=5;value=stopped;};};}",
            .State = EPipelineState::Stopped,
            .StaticUpdateAllowed = true,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
            .FlowCoreTargetUpdateAllowed = true,
        },
        {
            .Name = "StaleFlowView",
            .ImportantVersions = "{pipeline_spec_versions=10;pipeline_state_versions=7;flow_core_target_version=0;}",
            .FlowView = "{execution_spec={pipeline_state={version=5;value=stopped;};};}",
            .StateVersionMismatch = true,
        },
        {
            .Name = "FlowViewNewerThanImportantVersions",
            .ImportantVersions = ImportantVersions,
            .FlowView = "{execution_spec={pipeline_state={version=7;value=working;};};}",
            .StateVersionMismatch = true,
        },
        {
            .Name = "NothingPersisted",
            .State = EPipelineState::Unknown,
            .StaticUpdateAllowed = true,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
            .FlowCoreTargetUpdateAllowed = true,
        },
        {
            .Name = "SpecPersistedButNeverApplied",
            .ImportantVersions = "{pipeline_spec_versions=10;pipeline_state_versions=0;flow_core_target_version=0;}",
            .State = EPipelineState::Unknown,
            .StaticUpdateAllowed = true,
            .StaticUpdateAllowedWithForce = true,
            .DynamicOnlyUpdateAllowed = true,
            .FlowCoreTargetUpdateAllowed = true,
        },
        {
            .Name = "Completed",
            .ImportantVersions = ImportantVersions,
            .Replica = "{version=5;value=completed;}",
            .State = EPipelineState::Completed,
            .DynamicOnlyUpdateAllowed = true,
            .ReadsFlowView = false,
        },
        {
            .Name = "UnreadableFlowViewState",
            .ImportantVersions = ImportantVersions,
            .FlowView = "{execution_spec={pipeline_state={version=5;value=garbage;};};}",
        },
        {
            .Name = "UnreadableImportantVersions",
            .ImportantVersions = "garbage",
            .Replica = "{version=5;value=stopped;}",
        },
    };

    auto isAllowed = [] (auto&& check) {
        try {
            check();
            return true;
        } catch (const std::exception&) {
            return false;
        }
    };

    for (const auto& testCase : cases) {
        SCOPED_TRACE(testCase.Name);
        const THashMap<std::string, std::optional<TStringBuf>> rows{
            {"important_versions", testCase.ImportantVersions},
            {"pipeline_state_replica", testCase.Replica},
            {"flow_view", testCase.FlowView},
        };
        int flowViewReads = 0;
        auto observe = [&] {
            return ResolvePipelineState([&] (const std::vector<std::string>& keys) {
                std::vector<std::optional<TYsonString>> values;
                for (const auto& key : keys) {
                    flowViewReads += key == "flow_view";
                    const auto& value = GetOrCrash(rows, key);
                    values.push_back(value ? std::make_optional(TYsonString(TYsonStringBuf(*value))) : std::nullopt);
                }

                return values;
            });
        };

        if (testCase.StateVersionMismatch) {
            EXPECT_THROW_WITH_SUBSTRING(observe(), "differs from the important pipeline state version");
            EXPECT_EQ(flowViewReads, 1);
            continue;
        }

        if (!testCase.State) {
            EXPECT_ANY_THROW(observe());
            continue;
        }

        auto observed = observe();
        EXPECT_EQ(flowViewReads, testCase.ReadsFlowView ? 1 : 0);
        EXPECT_EQ(observed.State, *testCase.State);
        // get-pipeline-state reports the resolved state as is.
        EXPECT_TRUE(observed.Synced);
        EXPECT_EQ(
            isAllowed([&] {
                ValidatePipelineSafeForSpecUpdate(observed, /*staticSpecChanged*/ true, /*force*/ false);
            }),
            testCase.StaticUpdateAllowed);
        EXPECT_EQ(
            isAllowed([&] {
                ValidatePipelineSafeForSpecUpdate(observed, /*staticSpecChanged*/ true, /*force*/ true);
            }),
            testCase.StaticUpdateAllowedWithForce);
        EXPECT_EQ(
            isAllowed([&] {
                ValidatePipelineSafeForSpecUpdate(observed, /*staticSpecChanged*/ false, /*force*/ false);
            }),
            testCase.DynamicOnlyUpdateAllowed);
        EXPECT_EQ(
            isAllowed([&] {
                ValidatePipelineSafeForFlowCoreTargetUpdate(observed, /*allowOnPause*/ false);
            }),
            testCase.FlowCoreTargetUpdateAllowed);
    }
}

////////////////////////////////////////////////////////////////////////////////

// Parses but fails ValidatePipelineSpec.
constexpr TStringBuf InvalidPipelineSpecValue = "{streams={input={class_name=UnregisteredClassName;schema=[{name=value;type=string;};];};};}";

struct TEmbeddedSpecRepairCase
{
    std::string Name;
    EPipelineState PipelineState = EPipelineState::Stopped;
    std::string EmbeddedSpec;
    std::string EmbeddedDynamicSpec;
    std::string CanonicalSpec;
    std::string CanonicalDynamicSpec;

    bool Throws = false;
    bool SpecReplaced = false;
    bool DynamicSpecReplaced = false;
};

// Only embedded specs that fail to parse are replaced; validation errors are reported by the controller.
TEST(TRepairEmbeddedPipelineSpecsTest, RepairEmbeddedPipelineSpecsContract)
{
    const std::string validSpec = "{version=5;value={binary_version=embedded;};}";
    const std::string unparsableSpec = "{version=5;value={computations=legacy_invalid;};}";
    const std::string invalidSpec = Format("{version=5;value=%v;}", InvalidPipelineSpecValue);
    const std::string validDynamicSpec = "{version=5;value={};}";
    const std::string unparsableDynamicSpec = "{version=5;value={computations=legacy_invalid;};}";
    const std::string canonicalSpec = "{version=7;value={binary_version=canonical;};}";
    const std::string canonicalDynamicSpec = "{version=7;value={controller_connector={controller_wait_timeout=1234;};};}";

    const std::vector<TEmbeddedSpecRepairCase> cases = {
        {
            .Name = "Valid",
            .PipelineState = EPipelineState::Working,
            .EmbeddedSpec = validSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
        },
        {
            .Name = "NothingAppliedYet",
            .PipelineState = EPipelineState::Working,
            .EmbeddedSpec = Format("{version=0;value=%v;}", InvalidPipelineSpecValue),
            .EmbeddedDynamicSpec = "{version=0;value={};}",
        },
        {
            .Name = "UnparsableSpecOnStopped",
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
            .SpecReplaced = true,
        },
        {
            .Name = "UnparsableSpecOnPaused",
            .PipelineState = EPipelineState::Paused,
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
            .SpecReplaced = true,
        },
        {
            .Name = "UnparsableSpecOnWorking",
            .PipelineState = EPipelineState::Working,
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
            .Throws = true,
        },
        {
            .Name = "InvalidSpecOnStopped",
            .EmbeddedSpec = invalidSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
        },
        {
            .Name = "InvalidSpecOnWorking",
            .PipelineState = EPipelineState::Working,
            .EmbeddedSpec = invalidSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
        },
        {
            .Name = "InvalidCanonicalSpec",
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
            .CanonicalSpec = Format("{version=7;value=%v;}", InvalidPipelineSpecValue),
            .SpecReplaced = true,
        },
        {
            .Name = "UnparsableCanonicalSpec",
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
            .CanonicalSpec = "{version=7;value={computations=legacy_invalid;};}",
            .Throws = true,
        },
        {
            .Name = "CanonicalSpecNotNewerThanUnparsable",
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = validDynamicSpec,
            .CanonicalSpec = "{version=5;value={binary_version=canonical;};}",
            .Throws = true,
        },
        {
            .Name = "UnparsableDynamicSpecOnWorking",
            .PipelineState = EPipelineState::Working,
            .EmbeddedSpec = validSpec,
            .EmbeddedDynamicSpec = unparsableDynamicSpec,
            .DynamicSpecReplaced = true,
        },
        {
            .Name = "UnparsableSpecsOnStopped",
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = unparsableDynamicSpec,
            .SpecReplaced = true,
            .DynamicSpecReplaced = true,
        },
        {
            // The dynamic spec is not replaced alone when the static one cannot be.
            .Name = "UnparsableSpecsOnWorking",
            .PipelineState = EPipelineState::Working,
            .EmbeddedSpec = unparsableSpec,
            .EmbeddedDynamicSpec = unparsableDynamicSpec,
            .Throws = true,
        },
        {
            .Name = "InvalidSpecAndUnparsableDynamicSpecOnWorking",
            .PipelineState = EPipelineState::Working,
            .EmbeddedSpec = invalidSpec,
            .EmbeddedDynamicSpec = unparsableDynamicSpec,
            .DynamicSpecReplaced = true,
        },
    };

    for (const auto& testCase : cases) {
        SCOPED_TRACE(testCase.Name);
        auto toNode = [] (TStringBuf yson) {
            return ConvertToNode(TYsonStringBuf(yson));
        };
        constexpr TStringBuf FlowStateFormat = "{execution_spec={pipeline_state={version=1;value=%lv;};pipeline_spec=%v;dynamic_pipeline_spec=%v;};}";
        auto flowStateNode = toNode(Format(FlowStateFormat, testCase.PipelineState, testCase.EmbeddedSpec, testCase.EmbeddedDynamicSpec));
        const auto originalNode = CloneNode(flowStateNode);
        auto canonicalSpecNode = toNode(testCase.CanonicalSpec.empty() ? canonicalSpec : testCase.CanonicalSpec);
        auto canonicalDynamicSpecNode = toNode(testCase.CanonicalDynamicSpec.empty() ? canonicalDynamicSpec : testCase.CanonicalDynamicSpec);

        auto repair = [&] {
            RepairEmbeddedPipelineSpecs(flowStateNode, canonicalSpecNode, canonicalDynamicSpecNode);
        };

        if (testCase.Throws) {
            EXPECT_THROW_WITH_SUBSTRING(repair(), "Cannot recover the pipeline specs embedded in FlowState");
            EXPECT_TRUE(AreNodesEqual(flowStateNode, originalNode));
            continue;
        }

        repair();
        EXPECT_NO_THROW(ConvertTo<TFlowStatePtr>(flowStateNode));
        auto expectSpec = [&] (TStringBuf path, const INodePtr& canonical, bool replaced) {
            SCOPED_TRACE(path);
            auto actual = GetNodeByYPath(flowStateNode, TYPath(path));
            auto original = GetNodeByYPath(originalNode, TYPath(path));
            if (!replaced) {
                EXPECT_TRUE(AreNodesEqual(actual, original));
                return;
            }

            EXPECT_TRUE(AreNodesEqual(actual->AsMap()->GetChildOrThrow("value"), canonical->AsMap()->GetChildOrThrow("value")));
            auto actualVersion = ConvertTo<TVersion>(actual->AsMap()->GetChildOrThrow("version"));
            EXPECT_EQ(actualVersion, ConvertTo<TVersion>(original->AsMap()->GetChildOrThrow("version")));
            EXPECT_LT(actualVersion, ConvertTo<TVersion>(canonical->AsMap()->GetChildOrThrow("version")));
        };
        expectSpec("/execution_spec/pipeline_spec", canonicalSpecNode, testCase.SpecReplaced);
        expectSpec("/execution_spec/dynamic_pipeline_spec", canonicalDynamicSpecNode, testCase.DynamicSpecReplaced);
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, PipelineStateCommandsWithoutFlowView)
{
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    const auto& state = PersistedStateManagerLocalState;
    SeedPersistedSpec("{binary_version=legacy;}");
    state->FlowCoreTarget->TrySetValue(TFlowCoreTarget(std::string("legacy_target")), TestStateVersionProvider);
    SetPersistedPipelineState(EPipelineState::Stopped);
    // The canonical spec row cannot be parsed by this controller.
    auto legacySpecRow = Format("{version=%v;value={computations=legacy_invalid;};}", state->Spec->GetVersion());
    state->SpecNode = ConvertToNode(TYsonStringBuf(legacySpecRow));
    const auto legacySpecVersion = state->Spec->GetVersion();

    Prepare();

    ExecuteViaControlQueue([&] {
        StartLeadingWithoutFlowView();
        auto stopLeading = Finally([&] {
            StopLeading();
        });
        auto executor = CreateTestFlowExecutor();

        auto nativeState = WaitFor(ControllerServiceProxy->GetPipelineState()->Invoke()).ValueOrThrow();
        EXPECT_EQ(static_cast<EPipelineState>(nativeState->state()), EPipelineState::Stopped);
        auto flowExecuteState = ConvertToNode(ExecuteCommand(executor, "get-pipeline-state", "{}"));
        EXPECT_EQ(
            ConvertTo<EPipelineState>(flowExecuteState->AsMap()->GetChildOrThrow("pipeline_state")),
            EPipelineState::Stopped);

        // Commands outside the recovery allow-list still need the FlowView.
        EXPECT_THROW_WITH_ERROR_CODE(
            ExecuteCommand(executor, "get-flow-view", "{}"),
            NFlow::EErrorCode::FlowViewKeeperIsNotInitialized);

        ExecuteCommand(executor, "set-flow-core-target", "{flow_core_target=\"\";}");
        {
            auto guard = Guard(state->Lock);
            EXPECT_EQ(state->FlowCoreTarget->GetValue(), TFlowCoreTarget());
        }

        auto specsResult = executor->SetPipelineSpecs(MakeSetPipelineSpecsArg("repaired", 1234));
        {
            auto guard = Guard(state->Lock);
            EXPECT_GT(state->Spec->GetVersion(), legacySpecVersion);
            EXPECT_EQ(state->Spec->GetVersion(), specsResult.SpecVersion);
            EXPECT_EQ(state->Spec->GetValue()->BinaryVersion, "repaired");
            EXPECT_EQ(state->DynamicSpec->GetVersion(), specsResult.DynamicSpecVersion);
        }

        WaitFor(ControllerServiceProxy->StartPipeline()->Invoke()).ThrowOnError();
        {
            auto guard = Guard(state->Lock);
            EXPECT_EQ(state->DynamicSpec->GetValue()->TargetState, EPipelineState::Completed);
        }

        EXPECT_FALSE(Controller->GetFlowViewKeeper()->TryGetFlowView());
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, SetTargetPipelineStateWithoutFlowView)
{
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    SeedPersistedSpec("{binary_version=legacy;}");
    SetPersistedPipelineState(EPipelineState::Stopped);

    Prepare();

    const auto& state = PersistedStateManagerLocalState;
    auto getTargetState = [&] {
        auto guard = Guard(state->Lock);
        return std::pair(state->DynamicSpec->GetValue()->TargetState, state->DynamicSpec->GetVersion());
    };

    ExecuteViaControlQueue([&] {
        StartLeadingWithoutFlowView();
        auto stopLeading = Finally([&] {
            StopLeading();
        });
        auto executor = CreateTestFlowExecutor();

        ExecuteCommand(executor, "set-target-pipeline-state", "{target_pipeline_state=completed;}");
        EXPECT_EQ(getTargetState().first, EPipelineState::Completed);

        // Completed is final: the persisted state rejects any target change.
        SetPersistedPipelineState(EPipelineState::Completed);
        auto targetState = getTargetState();
        EXPECT_THROW_WITH_SUBSTRING(
            ExecuteCommand(executor, "set-target-pipeline-state", "{target_pipeline_state=stopped;}"),
            "Completed is a final state");
        EXPECT_EQ(getTargetState(), targetState);
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, SetPipelineSpecsRejectsUnparsableRequestedSpec)
{
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    SeedPersistedSpec("{binary_version=legacy;}");
    SetPersistedPipelineState(EPipelineState::Stopped);

    Prepare();

    const auto& state = PersistedStateManagerLocalState;
    auto getVersions = [&] {
        auto guard = Guard(state->Lock);
        return std::pair(state->Spec->GetVersion(), state->DynamicSpec->GetVersion());
    };

    ExecuteViaControlQueue([&] {
        StartLeadingWithoutFlowView();
        auto stopLeading = Finally([&] {
            StopLeading();
        });
        auto executor = CreateTestFlowExecutor();

        // The persisted rows parse, so the request is rejected as is and nothing is persisted.
        auto versions = getVersions();
        TSetPipelineSpecsArg argument;
        argument.Spec = ConvertToNode(TYsonStringBuf("{computations=legacy_invalid;}"));
        argument.DynamicSpec = ConvertToNode(TYsonStringBuf("{controller_connector=legacy_invalid;}"));
        EXPECT_ANY_THROW(executor->SetPipelineSpecs(argument));
        EXPECT_EQ(getVersions(), versions);
        auto guard = Guard(state->Lock);
        EXPECT_EQ(state->Spec->GetValue()->BinaryVersion, "legacy");
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, BrokenSpecsCanBeViewedWithoutFlowView)
{
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    SeedPersistedSpec("{binary_version=legacy;}");
    SeedPersistedDynamicSpec("{target_state=stopped;}");
    SetPersistedPipelineState(EPipelineState::Stopped);

    Prepare();

    // describe-pipeline resolves the flow tables bundle.
    EXPECT_CALL(*YTConnector, GetFlowTablesBundle())
        .WillRepeatedly(Return(MakeFuture(TFlowTablesBundleInfo{})));

    const auto& state = PersistedStateManagerLocalState;
    auto toNode = [] (TStringBuf yson) {
        return ConvertToNode(TYsonStringBuf(yson));
    };

    ExecuteViaControlQueue([&] {
        StartLeadingWithoutFlowView();
        auto stopLeading = Finally([&] {
            StopLeading();
        });
        auto executor = CreateTestFlowExecutor(RootStatusProfiler);

        auto getNativeSpecs = [&] {
            auto specRsp = WaitFor(ControllerServiceProxy->GetSpec()->Invoke()).ValueOrThrow();
            auto dynamicSpecRsp = WaitFor(ControllerServiceProxy->GetDynamicSpec()->Invoke()).ValueOrThrow();
            return std::tuple(
                toNode(specRsp->spec()),
                TVersion(specRsp->version()),
                toNode(dynamicSpecRsp->spec()),
                TVersion(dynamicSpecRsp->version()));
        };

        {
            SCOPED_TRACE("Parsable specs are returned typed");
            auto [spec, specVersion, dynamicSpec, dynamicSpecVersion] = getNativeSpecs();
            auto guard = Guard(state->Lock);
            EXPECT_TRUE(AreNodesEqual(spec, ConvertToNode(state->Spec->GetValue())));
            EXPECT_EQ(specVersion, state->Spec->GetVersion());
            EXPECT_TRUE(AreNodesEqual(dynamicSpec, ConvertToNode(state->DynamicSpec->GetValue())));
            EXPECT_EQ(dynamicSpecVersion, state->DynamicSpec->GetVersion());
        }

        // The canonical rows cannot be parsed by this controller.
        const auto rawSpec = toNode("{computations=legacy_invalid;}");
        const auto rawDynamicSpec = toNode("{controller_connector=legacy_invalid;}");
        const auto rawVersion = TVersion(12345);
        {
            auto guard = Guard(state->Lock);
            auto makeRow = [&] (const INodePtr& value) {
                // clang-format off
                return BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("version").Value(rawVersion)
                        .Item("value").Value(value)
                    .EndMap();
                // clang-format on
            };
            state->SpecNode = makeRow(rawSpec);
            state->DynamicSpecNode = makeRow(rawDynamicSpec);
        }

        {
            SCOPED_TRACE("Unparsable specs are returned raw");
            auto [spec, specVersion, dynamicSpec, dynamicSpecVersion] = getNativeSpecs();
            EXPECT_TRUE(AreNodesEqual(spec, rawSpec));
            EXPECT_EQ(specVersion, rawVersion);
            EXPECT_TRUE(AreNodesEqual(dynamicSpec, rawDynamicSpec));
            EXPECT_EQ(dynamicSpecVersion, rawVersion);
        }

        auto flowExecuteSpec = ConvertToNode(ExecuteCommand(executor, "get-pipeline-spec", "{path=\"/computations\";}"));
        EXPECT_TRUE(AreNodesEqual(flowExecuteSpec->AsMap()->GetChildOrThrow("spec"), rawSpec->AsMap()->GetChildOrThrow("computations")));
        auto flowExecuteDynamicSpec = ConvertToNode(ExecuteCommand(executor, "get-pipeline-dynamic-spec", "{}"));
        EXPECT_TRUE(AreNodesEqual(flowExecuteDynamicSpec->AsMap()->GetChildOrThrow("spec"), rawDynamicSpec));

        auto patch = New<TDynamicSpecPatch>();
        patch->Version = TVersion(123);
        auto& change = patch->Changes.emplace_back();
        change.Path = "/controller_connector";
        change.Value = toNode("{controller_wait_timeout=567;}");
        patch->Comment = "Keep the connector settings";
        patch->Timestamp = TInstant::Seconds(5);
        {
            auto guard = Guard(state->Lock);
            state->DynamicSpecNode->AsMap()->AddChild("patches", ConvertToNode(std::vector{patch}));
        }
        {
            SCOPED_TRACE("Raw base and persisted patches remain readable without a FlowView");
            auto [spec, specVersion, dynamicSpec, dynamicSpecVersion] = getNativeSpecs();
            EXPECT_TRUE(AreNodesEqual(spec, rawSpec));
            EXPECT_EQ(specVersion, rawVersion);
            EXPECT_TRUE(AreNodesEqual(dynamicSpec, toNode("{controller_connector={controller_wait_timeout=567;};}")));
            EXPECT_EQ(dynamicSpecVersion, rawVersion);

            auto base = ConvertTo<IMapNodePtr>(ExecuteCommand(executor, "get-pipeline-dynamic-spec", "{view=base;}"));
            EXPECT_TRUE(AreNodesEqual(base->GetChildOrThrow("spec"), rawDynamicSpec));
            auto effective = ConvertTo<IMapNodePtr>(ExecuteCommand(executor, "get-pipeline-dynamic-spec", "{blame=%true;}"));
            auto blame = effective->GetChildOrThrow("blame")->AsMap()->GetChildOrThrow("/controller_connector")->AsMap();
            EXPECT_EQ(blame->GetChildValueOrThrow<TVersion>("patch_id"), patch->Version);
            auto patches = ConvertTo<IMapNodePtr>(ExecuteCommand(executor, "get-pipeline-dynamic-spec-patches", "{}"));
            EXPECT_TRUE(AreNodesEqual(patches->GetChildOrThrow("patches"), ConvertToNode(std::vector{patch})));
        }

        // The status-only description carries the recovery error; the full one needs the FlowView.
        EXPECT_TRUE(WaitForCondition([&] {
            auto description = ConvertToYsonString(ExecuteCommand(executor, "describe-pipeline", "{status_only=%true;}"), EYsonFormat::Text);
            return description.ToString().Contains("Injected unrecoverable FlowState");
        }));
        EXPECT_THROW_WITH_ERROR_CODE(
            ExecuteCommand(executor, "describe-pipeline", "{}"),
            NFlow::EErrorCode::FlowViewKeeperIsNotInitialized);
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, PipelineStateCommandsWithoutFlowViewRejectUnsafeStates)
{
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    const auto& state = PersistedStateManagerLocalState;
    auto getVersions = [&] {
        auto guard = Guard(state->Lock);
        return std::tuple(state->Spec->GetVersion(), state->DynamicSpec->GetVersion(), state->FlowCoreTarget->GetVersion());
    };

    ExecuteViaControlQueue([&] {
        StartLeadingWithoutFlowView();
        auto stopLeading = Finally([&] {
            StopLeading();
        });
        auto executor = CreateTestFlowExecutor();

        auto expectRejected = [&] (int attempt) {
            auto versions = getVersions();
            EXPECT_THROW_WITH_SUBSTRING(
                executor->SetPipelineSpecs(MakeSetPipelineSpecsArg(Format("v%v", attempt), attempt)),
                "Cannot modify pipeline spec");
            TSetFlowCoreTargetArg flowCoreTargetArgument;
            flowCoreTargetArgument.FlowCoreTarget = TFlowCoreTarget(Format("target%v", attempt));
            EXPECT_THROW_WITH_SUBSTRING(
                executor->SetFlowCoreTarget(flowCoreTargetArgument),
                "Cannot update FlowCoreTarget");
            EXPECT_EQ(getVersions(), versions);
        };

        int attempt = 0;
        const std::vector<EPipelineState> unsafeStates = {
            EPipelineState::Working,
            EPipelineState::Draining,
            EPipelineState::Pausing,
            EPipelineState::Completed,
            EPipelineState::Paused,
        };
        for (auto pipelineState : unsafeStates) {
            SCOPED_TRACE(Format("%v", pipelineState));
            SetPersistedPipelineState(pipelineState);
            expectRejected(++attempt);
        }

        {
            SCOPED_TRACE("Replica is older than FlowState after a controller rollback");
            SetPersistedPipelineState(EPipelineState::Stopped);
            {
                auto guard = Guard(state->Lock);
                state->Replica = CloneYsonStruct(state->FlowView->State->ExecutionSpec->PipelineState);
            }

            SetPersistedPipelineState(EPipelineState::Working);
            expectRejected(++attempt);
            auto guard = Guard(state->Lock);
            state->Replica.Reset();
        }

        // A dynamic-only update is allowed in any state.
        auto dynamicSpecVersion = std::get<1>(getVersions());
        auto result = executor->SetPipelineSpecs(MakeSetPipelineSpecsArg(std::nullopt, 4321));
        EXPECT_GT(result.DynamicSpecVersion, dynamicSpecVersion);
        auto guard = Guard(state->Lock);
        EXPECT_EQ(state->DynamicSpec->GetVersion(), result.DynamicSpecVersion);
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, DynamicOnlySpecUpdateReplacesUnparsableDynamicSpec)
{
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    const auto& state = PersistedStateManagerLocalState;
    SeedPersistedSpec("{binary_version=legacy;}");
    SetPersistedPipelineState(EPipelineState::Working);
    // The canonical dynamic spec row cannot be parsed, and its version differs from the typed one.
    auto legacyDynamicSpec = New<TVersionedDynamicPipelineSpec>();
    legacyDynamicSpec->TrySetValue(
        ConvertTo<TDynamicPipelineSpecPtr>(TYsonStringBuf("{controller_connector={controller_wait_timeout=1;};}")),
        TestStateVersionProvider);
    const auto legacyDynamicSpecVersion = legacyDynamicSpec->GetVersion();
    ASSERT_NE(legacyDynamicSpecVersion, state->DynamicSpec->GetVersion());
    auto legacyDynamicSpecRow = Format("{version=%v;value={controller_connector=legacy_invalid;};}", legacyDynamicSpecVersion);
    state->DynamicSpecNode = ConvertToNode(TYsonStringBuf(legacyDynamicSpecRow));
    auto patch = New<TDynamicSpecPatch>();
    patch->Version = TVersion(123);
    auto& change = patch->Changes.emplace_back();
    change.Path = "/controller_connector/controller_wait_timeout";
    change.Value = ConvertToNode(7777);
    patch->Comment = "Keep the runtime timeout";
    patch->Timestamp = TInstant::Seconds(5);
    state->DynamicSpecNode->AsMap()->AddChild("patches", ConvertToNode(std::vector{patch}));
    const auto specVersion = state->Spec->GetVersion();

    Prepare();

    ExecuteViaControlQueue([&] {
        StartLeadingWithoutFlowView();
        auto stopLeading = Finally([&] {
            StopLeading();
        });
        auto executor = CreateTestFlowExecutor();

        auto result = executor->SetPipelineSpecs(MakeSetPipelineSpecsArg(std::nullopt, 4321));

        auto guard = Guard(state->Lock);
        EXPECT_FALSE(state->DynamicSpecNode);
        EXPECT_GT(result.DynamicSpecVersion, legacyDynamicSpecVersion);
        EXPECT_EQ(state->DynamicSpec->GetVersion(), result.DynamicSpecVersion);
        EXPECT_EQ(state->DynamicSpec->GetBaseSpec()->ControllerConnector->ControllerWaitTimeout, TDuration::MilliSeconds(4321));
        EXPECT_EQ(state->DynamicSpec->GetValue()->ControllerConnector->ControllerWaitTimeout, TDuration::MilliSeconds(7777));
        EXPECT_TRUE(AreNodesEqual(ConvertToNode(state->DynamicSpec->Patches), ConvertToNode(std::vector{patch})));
        EXPECT_EQ(state->Spec->GetVersion(), specVersion);
        EXPECT_EQ(state->Spec->GetValue()->BinaryVersion, "legacy");
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, SpecUpdateGateIsTransactional)
{
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    SetPersistedPipelineState(EPipelineState::Stopped);

    Prepare();

    // The pipeline starts right after the first update attempt has observed it stopped.
    auto observeCount = std::make_shared<std::atomic<int>>(0);
    EXPECT_CALL(*PersistedStateManager, RecoverPersistedPipelineState())
        .WillRepeatedly(
            [state = PersistedStateManagerLocalState, observeCount, versionProvider = TestStateVersionProvider] () {
                auto guard = Guard(state->Lock);
                auto observed = state->ObservePersistedPipelineState();
                if (observeCount->fetch_add(1) == 0) {
                    state->FlowView->State->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Working, versionProvider);
                }

                return observed;
            });

    const auto& state = PersistedStateManagerLocalState;
    const auto specVersion = state->Spec->GetVersion();

    ExecuteViaControlQueue([&] {
        StartLeadingWithoutFlowView();
        auto stopLeading = Finally([&] {
            StopLeading();
        });

        EXPECT_THROW_WITH_SUBSTRING(
            CreateTestFlowExecutor()->SetPipelineSpecs(MakeSetPipelineSpecsArg("new", std::nullopt)),
            "Cannot modify pipeline spec while pipeline is \"working\"");
        EXPECT_EQ(observeCount->load(), 2);
        auto guard = Guard(state->Lock);
        EXPECT_EQ(state->Spec->GetVersion(), specVersion);
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, DynamicOnlySpecUpdateOnWorkingPipeline)
{
    // No scheduler iteration runs, so the recovered FlowView stays Working.
    ControllerConfig->WarmUpTime = TDuration::Hours(1);

    SetPersistedPipelineState(EPipelineState::Working);

    Prepare();

    const auto& state = PersistedStateManagerLocalState;

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();
        auto stopLeading = Finally([&] {
            StopLeading();
        });
        auto executor = CreateTestFlowExecutor();

        const auto dynamicSpecVersion = state->DynamicSpec->GetVersion();
        // A request that also changes the static spec is rejected as a whole.
        EXPECT_THROW_WITH_SUBSTRING(
            executor->SetPipelineSpecs(MakeSetPipelineSpecsArg("new", 1234)),
            "Cannot modify pipeline spec while pipeline is \"working\"");
        {
            auto guard = Guard(state->Lock);
            EXPECT_EQ(state->DynamicSpec->GetVersion(), dynamicSpecVersion);
        }

        auto result = executor->SetPipelineSpecs(MakeSetPipelineSpecsArg(std::nullopt, 1234));
        EXPECT_GT(result.DynamicSpecVersion, dynamicSpecVersion);
        auto guard = Guard(state->Lock);
        EXPECT_EQ(state->DynamicSpec->GetVersion(), result.DynamicSpecVersion);
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, ValidationOnlySpecErrorPausesPipeline)
{
    ControllerConfig->WarmUpTime = TDuration::Zero();
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    // A newer controller rejects a spec that an older one accepted and ran.
    SeedPersistedSpec(InvalidPipelineSpecValue);
    SeedPersistedDynamicSpec("{target_state=completed;}");
    SetPersistedPipelineState(EPipelineState::Working);

    Prepare();

    auto hasSpecValidationError = [&] {
        return RootStatusProfiler->GetStatus().Errors.contains("/spec_validation");
    };

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();
        auto stopLeading = Finally([&] {
            StopLeading();
        });

        ASSERT_TRUE(WaitForCondition([&] {
            return GetPersistedPipelineState() == EPipelineState::Paused;
        }));
        EXPECT_TRUE(hasSpecValidationError());

        // Starting does not resume the pipeline while the spec is invalid.
        WaitFor(ControllerServiceProxy->StartPipeline()->Invoke()).ThrowOnError();
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
        EXPECT_EQ(GetPersistedPipelineState(), EPipelineState::Paused);
        EXPECT_TRUE(hasSpecValidationError());

        TSetPipelineSpecsArg argument;
        argument.Spec = ConvertToNode(TYsonStringBuf("{}"));
        argument.AllowSpecUpdateOnPause = true;
        CreateTestFlowExecutor()->SetPipelineSpecs(argument);

        EXPECT_TRUE(WaitForCondition([&] {
            return GetPersistedPipelineState() == EPipelineState::Working;
        }));
        EXPECT_FALSE(hasSpecValidationError());
    });
}

TEST_F(TControllerTest, ValidationOnlyDynamicSpecErrorIsFixedByDynamicOnlyUpdate)
{
    ControllerConfig->WarmUpTime = TDuration::Zero();
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    // Parses, but references an undeclared throttler.
    SeedPersistedSpec("{}");
    SeedPersistedDynamicSpec("{target_state=completed;computations={computation={input_rows_throttler_id=missing;};};}");
    SetPersistedPipelineState(EPipelineState::Working);

    Prepare();

    auto hasSpecValidationError = [&] {
        return RootStatusProfiler->GetStatus().Errors.contains("/spec_validation");
    };

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();
        auto stopLeading = Finally([&] {
            StopLeading();
        });

        ASSERT_TRUE(WaitForCondition([&] {
            return GetPersistedPipelineState() == EPipelineState::Paused;
        }));
        EXPECT_TRUE(hasSpecValidationError());

        // A dynamic-only update needs no force on a paused pipeline.
        TSetPipelineSpecsArg argument;
        argument.DynamicSpec = ConvertToNode(TYsonStringBuf("{target_state=completed;}"));
        CreateTestFlowExecutor()->SetPipelineSpecs(argument);

        EXPECT_TRUE(WaitForCondition([&] {
            return GetPersistedPipelineState() == EPipelineState::Working;
        }));
        EXPECT_FALSE(hasSpecValidationError());
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, WorkerExcludedFromSchedulingWhenFlowCoreTargetMismatches)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    // Set FlowCoreTarget to the controller's own binary checksum.
    PersistedStateManagerLocalState->FlowCoreTarget->TrySetValue(TFlowCoreTarget(GetBinaryChecksum()), TestVersionProvider());

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

    YT_TLOG_INFO("Start WorkerExcludedFromSchedulingWhenFlowCoreTargetMismatches");

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

    YT_TLOG_INFO("Start WorkerIncludedWhenFlowCoreTargetEmpty");

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
    PersistedStateManagerLocalState->FlowCoreTarget->TrySetValue(TFlowCoreTarget(GetBinaryChecksum()), TestVersionProvider());

    Prepare();

    // Worker without FlowCoreVersion (doesn't report it).
    TWorkerInfo oldWorker;
    oldWorker.RpcAddress = "old-worker.net:81";
    oldWorker.State = EWorkerState::Registered;
    oldWorker.FlowCoreVersion = "";
    oldWorker.IncarnationId = TIncarnationId(TGuid::Create());

    EXPECT_CALL(*WorkerTracker, GetWorkers())
        .WillRepeatedly(Return(std::vector<TWorkerInfo>{oldWorker}));

    YT_TLOG_INFO("Start WorkerWithEmptyFlowCoreVersionExcludedWhenTargetSet");

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

TEST_P(TFlowCoreTargetMismatchTest, PipelineIsPausedOnMismatch)
{
    const auto& param = GetParam();

    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    Prepare();

    auto versionProvider = New<TFakeVersionProvider>(InitialClockUnixTime - 1);

    PersistedStateManagerLocalState->FlowView->State->ExecutionSpec
        ->PipelineState->TrySetValue(param.PipelineState, versionProvider);
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->TargetState = param.TargetState;
    PersistedStateManagerLocalState->DynamicSpec->TrySetValue(dynamicSpec, versionProvider);

    PersistedStateManagerLocalState->FlowCoreTarget
        ->TrySetValue(TFlowCoreTarget(std::string("mismatch_version")), versionProvider);

    YT_TLOG_INFO("Start PipelineIsPausedOnMismatch")
        .With("PipelineState", param.PipelineState)
        .With("TargetState", param.TargetState);

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        auto getPipelineState = [state = PersistedStateManagerLocalState] {
            auto guard = Guard(state->Lock);
            return state->FlowView->State->ExecutionSpec->PipelineState->GetValue();
        };

        while (getPipelineState() != EPipelineState::Paused) {
            YT_TLOG_INFO("Waiting for pipeline to reach Paused after mismatch")
                .With("CurrentState", getPipelineState());
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

TEST_F(TControllerTest, WorkerIncarnationsJobsSurviveLateWorkerRegistration)
{
    ControllerConfig->WarmUpTime = TDuration::Zero();
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);

    auto schema = New<NTableClient::TTableSchema>(std::vector<NTableClient::TColumnSchema>{
        NTableClient::TColumnSchema("hash", NTableClient::EValueType::Uint64).SetRequired(true)});
    auto spec = New<TPipelineSpec>();
    auto computationSpec = New<TComputationSpec>();
    computationSpec->ComputationClassName = "NYT::NFlow::TPassthroughComputation";
    computationSpec->GroupBySchema = schema;
    computationSpec->InputStreamIds.insert("input_stream");
    computationSpec->OutputStreamIds.insert("output_stream");
    spec->Computations["computation"] = computationSpec;
    for (const auto& streamId : {TStreamId("input_stream"), TStreamId("output_stream")}) {
        spec->Streams[streamId] = New<TStreamSpec>();
        spec->Streams[streamId]->Schema = schema;
    }

    AddStreamProducer(spec, "input_stream");

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->TargetState = EPipelineState::Working;
    dynamicSpec->JobManager->AsyncBalancing = false;
    dynamicSpec->Computations["computation"] = New<TDynamicComputationSpec>();

    auto versionProvider = New<TFakeVersionProvider>(/*unixTime*/ 1);
    const auto& state = PersistedStateManagerLocalState;
    state->FlowView->State->AttachToControl(state->PersistedMasterControl);
    state->PersistedMasterControl->Recover();
    state->Spec->TrySetValue(spec, versionProvider);
    state->DynamicSpec->TrySetValue(dynamicSpec, versionProvider);
    state->FlowView->State->ExecutionSpec->PipelineSpec = CloneYsonStruct(state->Spec);
    state->FlowView->State->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(
        BuildExtendedPipelineSpec(spec),
        versionProvider);
    state->FlowView->State->ExecutionSpec->DynamicPipelineSpec = CloneYsonStruct(state->DynamicSpec);
    state->FlowView->State->ExecutionSpec->PipelineState->TrySetValue(
        EPipelineState::Working,
        versionProvider);

    TWorkerInfo earlyWorker;
    earlyWorker.RpcAddress = "early-worker.net:81";
    earlyWorker.State = EWorkerState::Registered;
    earlyWorker.IncarnationId = TIncarnationId(TGuid::Create());

    TWorkerInfo lateWorker;
    lateWorker.RpcAddress = "late-worker.net:81";
    lateWorker.State = EWorkerState::Registered;
    lateWorker.IncarnationId = TIncarnationId(TGuid::Create());

    auto createJob = [&] (const TWorkerInfo& worker) {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = TComputationId("computation");
        partition->State = EPartitionState::Executing;
        partition->StateTimestamp = TInstant::Now();
        state->FlowView->State->ExecutionSpec->Layout->CreatePartition(partition);

        auto job = New<TJob>();
        job->JobId = TJobId(TGuid::Create());
        job->PartitionId = partition->PartitionId;
        job->WorkerAddress = worker.RpcAddress;
        job->WorkerIncarnationId = worker.IncarnationId;
        state->FlowView->State->ExecutionSpec->Layout->CreateJob(job);
        return job->JobId;
    };
    state->FlowView->State->StartMutation();
    const auto earlyJobId = createJob(earlyWorker);
    const auto lateJobId = createJob(lateWorker);
    state->FlowView->State->CommitMutation();

    Prepare();

    // The leader address is never published, so the warm-up holds job management off for the
    // whole test and the recovered jobs stay in the layout.
    EXPECT_CALL(*YTConnector, GetLeadershipPublishTime())
        .WillRepeatedly(Return(TInstant::Zero()));

    // The recovered jobs have no leases; the leader grants them on the fixture transaction.
    EXPECT_CALL(*Transaction, GetId())
        .WillRepeatedly(Return(TGuid::Create()));
    EXPECT_CALL(*Transaction, SubscribeAborted(_))
        .Times(AnyNumber());
    EXPECT_CALL(*Transaction, Abort(_))
        .WillRepeatedly(Return(OKFuture));

    // The scheduler retains this action until #TearDown() joins its threads.
    auto lateWorkerRegistered = std::make_shared<std::atomic<bool>>(false);
    EXPECT_CALL(*WorkerTracker, GetWorkers())
        .WillRepeatedly([earlyWorker, lateWorker, lateWorkerRegistered] {
            std::vector<TWorkerInfo> workers{earlyWorker};
            if (lateWorkerRegistered->load()) {
                workers.push_back(lateWorker);
            }
            return workers;
        });

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();

        auto registeredWorkerCount = [&] {
            return std::ssize(Controller->GetFlowViewKeeper()->GetFlowView()->State->Workers);
        };
        auto waitForRegisteredWorkers = [&] (int count) {
            const auto deadline = TInstant::Now() + TDuration::Seconds(30);
            while (registeredWorkerCount() != count && TInstant::Now() < deadline) {
                TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
            }
            return registeredWorkerCount() == count;
        };

        auto expectIndexMatchesLayout = [&] {
            auto flowView = Controller->GetFlowViewKeeper()->GetFlowView();
            const auto& layout = flowView->State->ExecutionSpec->Layout;
            EXPECT_TRUE(layout->Jobs.contains(earlyJobId));
            EXPECT_TRUE(layout->Jobs.contains(lateJobId));

            THashMap<TIncarnationId, THashSet<TJobId>> expected;
            for (const auto& [jobId, job] : layout->Jobs) {
                expected[job->WorkerIncarnationId].insert(jobId);
            }
            EXPECT_EQ(flowView->EphemeralState->WorkerIncarnationsJobs, expected);
        };

        if (!waitForRegisteredWorkers(1)) {
            StopLeading();
            FAIL() << "The early worker was not registered";
        }
        expectIndexMatchesLayout();

        lateWorkerRegistered->store(true);
        if (!waitForRegisteredWorkers(2)) {
            StopLeading();
            FAIL() << "The late worker was not registered";
        }
        expectIndexMatchesLayout();

        StopLeading();
    });
}

////////////////////////////////////////////////////////////////////////////////

TEST(TWorkerTrackerServiceTest, HeartbeatDeliversDynamicSpecsOfLateWorkerJobs)
{
    using NYT::FromProto;
    using NYT::ToProto;

    auto localState = New<TPersistedStateManagerLocalState>();
    const auto& state = localState->FlowView->State;
    state->AttachToControl(localState->PersistedMasterControl);
    localState->PersistedMasterControl->Recover();

    auto registerWorker = [&] (const std::string& address) {
        auto worker = New<NFlow::TWorker>();
        worker->RpcAddress = address;
        worker->IncarnationId = TIncarnationId(TGuid::Create());
        state->Workers[address] = worker;
        return worker;
    };
    auto earlyWorker = registerWorker("early-worker.net:81");
    auto lateWorker = registerWorker("late-worker.net:81");

    auto ephemeralState = New<TFlowEphemeralState>();
    auto createJob = [&] (const NFlow::TWorkerPtr& worker) {
        auto partition = New<TPartition>();
        partition->PartitionId = TPartitionId(TGuid::Create());
        partition->ComputationId = TComputationId("computation");
        partition->State = EPartitionState::Executing;
        partition->StateTimestamp = TInstant::Now();
        state->ExecutionSpec->Layout->CreatePartition(partition);

        auto job = New<TJob>();
        job->JobId = TJobId(TGuid::Create());
        job->PartitionId = partition->PartitionId;
        job->WorkerAddress = worker->RpcAddress;
        job->WorkerIncarnationId = worker->IncarnationId;
        state->ExecutionSpec->Layout->CreateJob(job);

        ephemeralState->WorkerIncarnationsJobs[worker->IncarnationId].insert(job->JobId);
        const auto& dynamicPartitionSpec = ephemeralState->GetPartitionState(partition->PartitionId)->DynamicPartitionSpec;
        dynamicPartitionSpec->FinishAfterCurrentEpoch = worker == lateWorker;
        dynamicPartitionSpec->ComputationPartitionSpec = GetEphemeralNodeFactory()->CreateMap();
        dynamicPartitionSpec->ComputationPartitionSpec->AddChild("worker_address", ConvertToNode(worker->RpcAddress));
        return job->JobId;
    };
    state->StartMutation();
    createJob(earlyWorker);
    const auto lateJobId = createJob(lateWorker);
    state->CommitMutation();

    auto flowViewKeeper = New<TFlowViewKeeper>();
    flowViewKeeper->Init(state, ephemeralState, localState->Spec, localState->DynamicSpec);

    auto controller = New<StrictMock<TMockController>>();
    EXPECT_CALL(*controller, GetFlowViewKeeper())
        .WillRepeatedly(Return(flowViewKeeper));
    EXPECT_CALL(*controller, RegisterWorkerStatus(_, _))
        .Times(AnyNumber());

    TWorkerInfo lateWorkerInfo;
    static_cast<TNodeInfoBase&>(lateWorkerInfo) = static_cast<const TNodeInfoBase&>(*lateWorker);
    lateWorkerInfo.State = EWorkerState::Registered;
    auto workerTracker = New<StrictMock<TMockWorkerTracker>>();
    EXPECT_CALL(*workerTracker, HandleWorkerHeartbeat(lateWorker->RpcAddress, _, _))
        .WillOnce(Return(lateWorkerInfo));

    auto authenticator = New<StrictMock<TMockPipelineAuthenticator>>();
    EXPECT_CALL(*authenticator, CreateSelfRpcAuthenticator())
        .WillOnce(Return(nullptr));
    EXPECT_CALL(*authenticator, CreateSelfCredentialsInjectingChannelFactory(_))
        .WillOnce(Return(nullptr));

    auto actionQueue = New<TActionQueue>("HeartbeatTest");
    auto server = NRpc::CreateLocalServer();
    server->RegisterService(CreateWorkerTrackerService(
        controller,
        workerTracker,
        actionQueue->GetInvoker(),
        /*channelFactory*/ nullptr,
        authenticator));
    server->Start();

    TWorkerTrackerServiceProxy proxy(NRpc::CreateLocalChannel(server));
    auto request = proxy.Heartbeat();
    request->set_worker_rpc_address(lateWorker->RpcAddress);
    ToProto(request->mutable_connection_incarnation_id(), NWorker::TIncarnationId(TGuid::Create()));
    request->set_heartbeat_seq_no(1);
    request->set_execution_spec_versions(ToProto(ConvertToYsonString(New<TExecutionSpecVersions>())));
    request->set_worker_status(ToProto(ConvertToYsonString(New<TWorkerStatus>())));
    auto response = WaitFor(request->Invoke())
        .ValueOrThrow();

    ASSERT_EQ(response->jobs_dynamic_computation_partition_specs_size(), 1);
    const auto& jobSpec = response->jobs_dynamic_computation_partition_specs(0);
    EXPECT_EQ(FromProto<TJobId>(jobSpec.job_id()), lateJobId);
    auto deliveredSpec = ConvertTo<TDynamicPartitionSpecPtr>(TYsonStringBuf(jobSpec.spec()));
    EXPECT_TRUE(deliveredSpec->FinishAfterCurrentEpoch);
    ASSERT_TRUE(deliveredSpec->ComputationPartitionSpec);
    EXPECT_EQ(
        deliveredSpec->ComputationPartitionSpec->GetChildValueOrThrow<std::string>("worker_address"),
        lateWorker->RpcAddress);

    YT_UNUSED_FUTURE(server->Stop());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TControllerHelpersTest, LeadershipWarmupTimeout)
{
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    const auto& connector = dynamicSpec->ControllerConnector;

    // Derived from the periods that govern how long a worker needs to reconnect, so that raising
    // one of them raises the wait with it instead of leaving a fixed value meaningless.
    auto derived = [&] {
        return (connector->ControllerDiscoverPeriod + connector->ControllerHeartbeatPeriod) * 2 + TDuration::Seconds(5);
    };
    EXPECT_EQ(GetLeadershipWarmupTimeout(dynamicSpec), derived());

    connector->ControllerDiscoverPeriod = TDuration::Seconds(30);
    EXPECT_EQ(GetLeadershipWarmupTimeout(dynamicSpec), derived());
    EXPECT_GT(GetLeadershipWarmupTimeout(dynamicSpec), TDuration::Seconds(30));

    connector->ControllerHeartbeatPeriod = TDuration::Seconds(4);
    EXPECT_EQ(GetLeadershipWarmupTimeout(dynamicSpec), derived());

    // Waiting past the point where the workers abandon their jobs is pointless.
    connector->ControllerDiscoverPeriod = connector->ControllerWaitTimeout;
    EXPECT_EQ(GetLeadershipWarmupTimeout(dynamicSpec), connector->ControllerWaitTimeout);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TControllerHelpersTest, LeadershipWarmupLeft)
{
    auto now = TInstant::Now();

    auto flowView = New<TFlowView>();
    TTestDatabase database;
    auto control = New<TPersistedStateControl<std::string>>(New<TStorageHandler>(database));
    flowView->State->AttachToControl(control);
    control->Recover();

    auto dynamicSpec = New<TDynamicPipelineSpec>();
    flowView->CurrentDynamicSpec->TrySetValue(dynamicSpec, TestVersionProvider());
    const auto Warmup = GetLeadershipWarmupTimeout(dynamicSpec);
    ASSERT_GT(Warmup, TDuration::Seconds(5));

    const auto& layout = flowView->State->ExecutionSpec->Layout;

    // An empty layout has nothing to protect: a starting pipeline must not be delayed even before
    // the leader address is published.
    EXPECT_EQ(GetLeadershipWarmupLeft(flowView, /*publishTime*/ TInstant::Zero(), now), TDuration::Zero());

    auto partition = New<TPartition>();
    partition->PartitionId = TPartitionId(TGuid::Create());
    partition->ComputationId = TComputationId("computation");
    partition->State = EPartitionState::Executing;
    partition->StateTimestamp = now;
    auto job = New<TJob>();
    job->JobId = TJobId(TGuid::Create());
    job->PartitionId = partition->PartitionId;
    job->WorkerAddress = "worker.net:81";
    flowView->State->StartMutation();
    layout->CreatePartition(partition);
    layout->CreateJob(job);
    flowView->State->CommitMutation();

    // Not published yet: the workers cannot discover this leader at all, so the whole warm-up is
    // still ahead no matter how long ago leadership was won.
    EXPECT_EQ(GetLeadershipWarmupLeft(flowView, /*publishTime*/ TInstant::Zero(), now), Warmup);

    // Published: the window is counted from the publication, not from the leadership.
    EXPECT_EQ(GetLeadershipWarmupLeft(flowView, now - TDuration::Seconds(5), now), Warmup - TDuration::Seconds(5));
    EXPECT_EQ(GetLeadershipWarmupLeft(flowView, now - Warmup, now), TDuration::Zero());
    EXPECT_EQ(GetLeadershipWarmupLeft(flowView, now - TDuration::Hours(1), now), TDuration::Zero());

    // A pipeline whose workers are not expected to wait for the controller at all leaves nothing to
    // warm up for: the cap collapses the wait to zero.
    dynamicSpec->ControllerConnector->ControllerWaitTimeout = TDuration::Zero();
    flowView->CurrentDynamicSpec->TrySetValue(dynamicSpec, TestVersionProvider());
    EXPECT_EQ(GetLeadershipWarmupLeft(flowView, /*publishTime*/ TInstant::Zero(), now), TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TControllerTest, PausePublishesEmptyResourceLayoutWithoutWorkerAcknowledgement)
{
    ControllerConfig->WarmUpTime = TDuration::MilliSeconds(10);
    ControllerConfig->SchedulerPeriod = TDuration::MilliSeconds(10);
    ControllerConfig->FeedbackPeriod = TDuration::MilliSeconds(10);
    Prepare();
    TWorkerInfo worker;
    worker.RpcAddress = "worker";
    worker.IncarnationId = TIncarnationId(TGuid::Create());
    worker.State = EWorkerState::Registered;
    EXPECT_CALL(*YTConnector, GetLeadershipPublishTime())
        .WillRepeatedly(Return(TInstant::Now() - TDuration::Hours(1)));
    EXPECT_CALL(*WorkerTracker, GetWorkers())
        .WillRepeatedly(Return(std::vector<TWorkerInfo>{worker}));
    const auto& state = PersistedStateManagerLocalState;
    state->FlowView->State->AttachToControl(state->PersistedMasterControl);
    state->PersistedMasterControl->Recover();
    auto flowState = state->FlowView->State;
    auto versionProvider = New<TFakeVersionProvider>(InitialClockUnixTime - 1);
    flowState->ExecutionSpec->PipelineState->TrySetValue(EPipelineState::Pausing, versionProvider);
    auto dynamicSpec = New<TDynamicPipelineSpec>();
    dynamicSpec->TargetState = EPipelineState::Paused;
    state->DynamicSpec->TrySetValue(dynamicSpec, versionProvider);
    auto instance = New<TResourceInstance>();
    instance->ResourceInstanceId = TResourceInstanceId(TGuid::Create());
    instance->ResourceId = TResourceId("resource");
    instance->WorkerAddress = worker.RpcAddress;
    instance->WorkerIncarnationId = worker.IncarnationId;
    flowState->StartMutation();
    flowState->ExecutionSpec->Layout->ResourceInstances.emplace(instance->ResourceInstanceId, instance);
    flowState->CommitMutation();

    ExecuteViaControlQueue([&] {
        StartLeadingAndWaitReady();
        auto stop = Finally([&] {
            StopLeading();
        });
        auto status = New<TWorkerStatus>();
        status->WorkerIncarnationId = worker.IncarnationId;
        auto resourceStatus = New<TWorkerResourceStatus>();
        resourceStatus->ResourceId = instance->ResourceId;
        resourceStatus->LoadState = EResourceLoadState::Loaded;
        status->ResourceInstanceStatuses.emplace(instance->ResourceInstanceId, resourceStatus);
        Controller->RegisterWorkerStatus(worker.RpcAddress, status);
        const auto deadline = TInstant::Now() + TDuration::Seconds(3);
        auto current = [&] {
            return Controller->GetFlowViewKeeper()->GetFlowView()->State;
        };
        while (current()->ExecutionSpec->PipelineState->GetValue() != EPipelineState::Paused && TInstant::Now() < deadline) {
            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
        }
        EXPECT_EQ(current()->ExecutionSpec->PipelineState->GetValue(), EPipelineState::Paused);
        EXPECT_EQ(current()->ExecutionSpec->Layout->ResourceInstances.size(), 0u);
        EXPECT_EQ(current()->ExecutionSpec->Layout->Jobs.size(), 0u);
        EXPECT_EQ(current()->ExecutionSpec->Layout->WorkerSpecs.size(), 0u);
    });
}

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
    flowView->CurrentSpec->TrySetValue(spec, TestVersionProvider());
    flowView->CurrentDynamicSpec->TrySetValue(dynamicSpec, TestVersionProvider());
    flowView->State->ExecutionSpec->PipelineSpec->TrySetValue(spec, TestVersionProvider());
    flowView->State->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(BuildExtendedPipelineSpec(spec), TestVersionProvider());
    flowView->State->ExecutionSpec->DynamicPipelineSpec->TrySetValue(dynamicSpec, TestVersionProvider());
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

    flowView->State->ExecutionSpec->ExtendedPipelineSpec->TrySetValue(BuildExtendedPipelineSpec(spec), TestVersionProvider());
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

    YT_TLOG_INFO("Start SetFlowCoreTargetFailsOnSpecVersionMismatch");

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

    YT_TLOG_INFO("Start SetFlowCoreTargetFailsOnFlowCoreTargetVersionMismatch");

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

    YT_TLOG_INFO("Start SetFlowCoreTargetIsIdempotentForSameValue");

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
            target->TrySetValue(TFlowCoreTarget(std::string("mismatch_version")), TestVersionProvider());
            return target;
        });

    YT_TLOG_INFO("Start StartPipelineFailsOnFlowCoreTargetMismatch");

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

class TAuthorizeCommandTest
    : public TControllerTest
{
protected:
    const TIntrusivePtr<StrictMock<TMockClient>> Client_ = New<StrictMock<TMockClient>>();
    IFlowExecutorPtr FlowExecutor_;

    void SetUp() override
    {
        TControllerTest::SetUp();

        FlowExecutor_ = CreateFlowExecutor(
            Controller,
            PersistedStateManager,
            YTConnector,
            New<TControllerServiceConfig>(),
            /*orchidRoot*/ nullptr,
            CreateSyncStatusProfiler(),
            GetSyncInvoker());

        // The executor is built on the fixture's null client; only the permission check needs a real one.
        EXPECT_CALL(*YTConnector, GetClient())
            .WillRepeatedly(Return(Client_));
    }

    void ExpectPermissionCheck(EPermission permission, NSecurityClient::ESecurityAction action)
    {
        TCheckPermissionResponse response;
        response.Action = action;
        EXPECT_CALL(*Client_, CheckPermission("alice", NYPath::TYPath("//path"), permission, _))
            .WillOnce(Return(MakeFuture(response)));
    }
};

TEST_F(TAuthorizeCommandTest, ChecksThePermissionTheCommandRequires)
{
    ExpectPermissionCheck(EPermission::Read, NSecurityClient::ESecurityAction::Allow);
    FlowExecutor_->AuthorizeCommand("get-pipeline-state", "alice");

    ExpectPermissionCheck(EPermission::Write, NSecurityClient::ESecurityAction::Allow);
    FlowExecutor_->AuthorizeCommand("set-target-pipeline-state", "alice");
}

TEST_F(TAuthorizeCommandTest, RejectsDeniedUser)
{
    ExpectPermissionCheck(EPermission::Write, NSecurityClient::ESecurityAction::Deny);

    EXPECT_THROW_WITH_SUBSTRING(
        FlowExecutor_->AuthorizeCommand("set-target-pipeline-state", "alice"),
        "No \"write\" permission for pipeline //path");
}

TEST_F(TAuthorizeCommandTest, RejectsUnknownCommand)
{
    // No permission check expectation: an unknown command fails before the cluster is asked.
    EXPECT_THROW_WITH_SUBSTRING(
        FlowExecutor_->AuthorizeCommand("no-such-command", "alice"),
        "No such command: no-such-command, possible commands:");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NController
