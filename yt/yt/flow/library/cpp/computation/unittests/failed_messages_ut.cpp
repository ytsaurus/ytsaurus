#include <yt/yt/flow/library/cpp/computation/swift_map_computation.h>
#include <yt/yt/flow/library/cpp/computation/transform_computation.h>

#include <yt/yt/flow/library/cpp/common/partition_buffer_state.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/misc/load_throughput_throttler.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/rowset.h>

#include <yt/yt/client/hedging/unittests/mock/cache.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/client/unittests/mock/client.h>
#include <yt/yt/client/unittests/mock/timestamp_provider.h>
#include <yt/yt/client/unittests/mock/transaction.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace testing;

const TStreamId InputStreamId("input");

DEFINE_ENUM(ECommitOutcome,
    (Success)
    (Failure)
    (Uncertain)
);

struct TCaptureProbe
{
    ECommitOutcome Outcome = ECommitOutcome::Success;
    bool Captured = false;
    bool PrematureAcknowledgement = false;
    int Acknowledged = 0;
    int CaptureAttempts = 0;
    ui64 FailureTimestamp = 0;
    TTimestamp LastCommittedTimestamp = TTimestamp(0);
};

class TCaptureRunContext
    : public IComputationRunContext
{
public:
    TCaptureRunContext(TCaptureProbe* probe, TTableSchemaPtr schema)
        : Probe_(probe)
        , Schema_(std::move(schema))
    { }

    TFuture<std::vector<TInputMessageConstPtr>> GetNextBatch(const THashSet<TStreamId>&) override
    {
        if (Read_) {
            return MakeFuture<std::vector<TInputMessageConstPtr>>(TError("Finished capture test"));
        }
        Read_ = true;
        TMessageBuilder builder(InputStreamId, Schema_);
        builder.SetMessageId(TMessageId("failed-input"));
        builder.SetSystemTimestamp(TSystemTimestamp(1));
        builder.SetAlignmentTimestamp(TSystemTimestamp(1));
        builder.SetEventTimestamp(TSystemTimestamp(1));
        return MakeFuture(std::vector<TInputMessageConstPtr>{New<TInputMessage>(builder.Finish(), MakeKey(ui64(1)))});
    }

    TFuture<THashMap<TStreamId, TInflightMetricsPtr>> GetInputInflightMetrics() override
    {
        auto metrics = New<TInflightMetrics>();
        metrics->Count = 1;
        return MakeFuture(THashMap<TStreamId, TInflightMetricsPtr>{{InputStreamId, metrics}});
    }

    void MarkPersisted(std::span<const TMessageId> messages) override
    {
        Probe_->Acknowledged += std::ssize(messages);
        Probe_->PrematureAcknowledgement |= !messages.empty() && !Probe_->Captured;
    }

    void MarkDeduplicated(std::span<const TMessageId>) override
    { }

    void RegisterOutputMessages(std::span<const TOutputMessageConstPtr>, std::span<TDistributingTracker>) override
    { }

    void Commit() override
    { }

    TSystemTimestamp GetInputStabilizedEventTimestamp() const override
    {
        return ZeroSystemTimestamp;
    }

private:
    TCaptureProbe* const Probe_;
    const TTableSchemaPtr Schema_;
    bool Read_ = false;
};

class TCaptureTransform
    : public TTransformComputation
{
public:
    using TTransformComputation::TTransformComputation;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        output->AddFailedMessage(input->GetMessages().at(0), TError("Invalid input"));
    }
};

class TCaptureSwiftMap
    : public TSwiftMapComputation
{
public:
    using TSwiftMapComputation::TSwiftMapComputation;

    void DoProcess(IInputContextPtr input, IOutputCollectorPtr output) override
    {
        output->AddFailedMessage(input->GetMessages().at(0), TError("Invalid input"));
    }
};

YT_FLOW_DEFINE_COMPUTATION(TCaptureTransform);
YT_FLOW_DEFINE_COMPUTATION(TCaptureSwiftMap);

TError RunCapture(TCaptureProbe* probe, bool swift, bool enabled = true)
{
    auto queue = New<TActionQueue>("CaptureTest");
    auto timestampProvider = New<NiceMock<TMockTimestampProvider>>();
    auto nextTimestamp = std::make_shared<ui64>(1ULL << 32);
    ON_CALL(*timestampProvider, GenerateTimestamps(_, _))
        .WillByDefault([nextTimestamp] (auto, auto) {
            return MakeFuture(TTimestamp((*nextTimestamp)++));
        });
    auto client = New<NiceMock<TMockClient>>();
    client->SetTimestampProvider(timestampProvider);
    auto timerSchema = New<TTableSchema>(std::vector{
        TColumnSchema("computation_id", EValueType::String),
        TColumnSchema("key", EValueType::Any),
        TColumnSchema("message_id", EValueType::String),
        TColumnSchema("stream_id", EValueType::String),
        TColumnSchema("system_timestamp", EValueType::Int64),
        TColumnSchema("event_timestamp", EValueType::Int64),
        TColumnSchema("trigger_timestamp", EValueType::Int64),
    });
    ON_CALL(*client, SelectRows(_, _))
        .WillByDefault(Return(MakeFuture(TSelectRowsResult{
            .Rowset = CreateRowset(timerSchema, MakeSharedRange(std::vector<TUnversionedRow>{})),
        })));
    ON_CALL(*client, LookupRows(_, _, _, _))
        .WillByDefault([probe] (const auto& path, const auto&, const auto& keys, const auto&) {
            if (path == "//pipeline/partition_transactions") {
                auto schema = New<TTableSchema>(std::vector{TColumnSchema("last_transaction_start_timestamp", EValueType::Uint64)});
                auto buffer = New<TRowBuffer>();
                auto row = buffer->AllocateUnversioned(1);
                row[0] = MakeUnversionedUint64Value(probe->LastCommittedTimestamp.Underlying(), 0);
                return MakeFuture(TUnversionedLookupRowsResult{
                    .Rowset = CreateRowset(schema, MakeSharedRange(std::vector<TUnversionedRow>{row}, buffer)),
                });
            }
            return MakeFuture(TUnversionedLookupRowsResult{
                .Rowset = CreateRowset(New<TTableSchema>(), MakeSharedRange(std::vector<TUnversionedRow>(keys.Size()))),
            });
        });
    ON_CALL(*client, StartTransaction(_, _))
        .WillByDefault([probe, nextTimestamp] (auto, auto) {
            auto transaction = New<NiceMock<TMockTransaction>>();
            transaction->StartTimestamp = TTimestamp((*nextTimestamp)++);
            auto hasCapture = std::make_shared<bool>(false);
            ON_CALL(*transaction, LookupRows(_, _, _, _))
                .WillByDefault([] (const auto&, const auto&, const auto& keys, const auto&) {
                    return MakeFuture(TUnversionedLookupRowsResult{
                        .Rowset = CreateRowset(New<TTableSchema>(), MakeSharedRange(std::vector<TUnversionedRow>(keys.Size()))),
                    });
                });
            ON_CALL(*transaction, ModifyRows(_, _, _, _))
                .WillByDefault([probe, hasCapture] (const auto& path, const auto& nameTable, const auto& modifications, const auto&) {
                    if (path != "//pipeline/failed_messages") {
                        return;
                    }
                    *hasCapture = true;
                    ASSERT_EQ(modifications.Size(), 1u);
                    const auto row = std::get<NRowModifications::TWriteRow>(modifications[0]).Row;
                    const auto columnId = nameTable->GetIdOrThrow("failed_at");
                    const auto streamSpecIdColumnId = nameTable->GetIdOrThrow("stream_spec_id");
                    const auto diagnosticColumnId = nameTable->GetIdOrThrow("diagnostic");
                    for (const auto& value : row) {
                        if (value.Id == columnId) {
                            probe->FailureTimestamp = value.Data.Uint64;
                        }
                        if (value.Id == streamSpecIdColumnId) {
                            EXPECT_EQ(value.Data.Int64, 1);
                        }
                        if (value.Id == diagnosticColumnId) {
                            auto diagnostic = ConvertToNode(TYsonString(value.AsStringBuf()))->AsMap();
                            EXPECT_EQ(diagnostic->GetChildValueOrThrow<std::string>("worker_id"), "capture-worker:9012");
                            EXPECT_EQ(diagnostic->GetChildValueOrThrow<std::string>("flow_core_target"), "capture-core-target");
                        }
                    }
                });
            ON_CALL(*transaction, Commit(_))
                .WillByDefault([probe, hasCapture, timestamp = transaction->StartTimestamp] (const auto&) {
                    if (*hasCapture) {
                        ++probe->CaptureAttempts;
                        EXPECT_EQ(probe->Acknowledged, 0);
                        if (probe->Outcome == ECommitOutcome::Failure) {
                            return MakeFuture<TTransactionCommitResult>(TError(NYT::EErrorCode::Canceled, "Capture commit rejected"));
                        }
                        probe->Captured = true;
                    }
                    probe->LastCommittedTimestamp = timestamp;
                    if (*hasCapture && probe->Outcome == ECommitOutcome::Uncertain) {
                        return MakeFuture<TTransactionCommitResult>(TError(NYT::EErrorCode::Timeout, "Response lost after commit"));
                    }
                    return MakeFuture(TTransactionCommitResult{});
                });
            return MakeFuture<ITransactionPtr>(transaction);
        });
    auto clientsCache = New<NiceMock<NClient::NHedging::TMockClientsCache>>();
    ON_CALL(*clientsCache, GetClient(_)).WillByDefault(Return(client));
    auto spec = New<TComputationSpec>();
    spec->ComputationClassName = swift ? TypeName<TCaptureSwiftMap>() : TypeName<TCaptureTransform>();
    spec->InputStreamIds = {InputStreamId};
    spec->Parameters = ConvertTo<IMapNodePtr>(New<TPartitioningSpec>());
    auto context = New<TComputationContext>();
    context->ComputationSpec = spec;
    context->FailureHandlerEnabled = enabled;
    context->FlowCoreTarget = TFlowCoreTarget("capture-core-target");
    context->ClientsCache = clientsCache;
    context->PipelinePath = NYPath::TRichYPath("//pipeline");
    context->PipelinePath.SetCluster("test");
    context->Partition = New<TPartition>();
    context->Partition->ComputationId = TComputationId("capture");
    context->Partition->State = EPartitionState::Executing;
    context->Partition->PartitionId = TPartitionId(TGuid::Create());
    context->Job = New<TJob>();
    context->Job->WorkerAddress = "capture-worker:9012";
    context->Job->JobId = TJobId(TGuid::Create());
    context->Job->Generation = TUniqueSeqNo(42);
    context->Job->PartitionId = context->Partition->PartitionId;
    context->Job->LeaseId = TLeaseId(TGuid::Create());
    context->SerializedInvoker = queue->GetInvoker();
    context->PoolInvoker = queue->GetInvoker();
    context->Logger = NLogging::TLogger("CaptureTest");
    context->StatusProfiler = CreateSyncStatusProfiler();
    context->LoadThroughputThrottler = New<TLoadThroughputThrottler>("CaptureTest", context->Logger, context->Profiler);
    context->PartitionBufferState = CreateDetachedPartitionBufferState({});
    auto streamSpec = New<TStreamSpec>();
    streamSpec->Schema = New<TTableSchema>();
    context->StreamSpecStorage = New<TComputationStreamSpecStorage>(
        New<TStreamSpecs>(THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>{{InputStreamId, {{TStreamSpecId(1), streamSpec}}}}),
        New<TTableSchema>(),
        nullptr);
    context->DistributedThrottlerControllerChannelProvider = [] {
        return NRpc::IChannelPtr{};
    };
    auto dynamic = New<TDynamicComputationContext>();
    dynamic->DynamicComputationSpec = New<TDynamicComputationSpec>();
    dynamic->DynamicPartitionSpec = ConvertTo<TDynamicPartitionSpecPtr>(TYsonStringBuf("{computation_partition_spec={};}"));
    return WaitFor(BIND([=] {
        TIntrusivePtr<TUniversalComputationBase> computation;
        if (swift) {
            computation = New<TCaptureSwiftMap>(context, dynamic);
        } else {
            computation = New<TCaptureTransform>(context, dynamic);
        }
        computation->UpdateWatermarkState(New<TWatermarkState>());
        auto input = New<TStreamTraverseData>();
        input->State = EStreamState::Active;
        input->InflightMetrics = New<TInflightMetrics>();
        computation->SetInputTraverse({{InputStreamId, input}});
        static_cast<IComputation*>(computation.Get())->Run(New<TCaptureRunContext>(probe, context->StreamSpecStorage->GetSchema(InputStreamId)));
    }).AsyncVia(queue->GetInvoker())
            .Run());
}

TEST(TFailedMessageCaptureTest, AcknowledgesOnlyAfterSuccessfulOrRecoveredCommit)
{
    for (auto outcome : {ECommitOutcome::Success, ECommitOutcome::Failure, ECommitOutcome::Uncertain}) {
        TCaptureProbe probe;
        probe.Outcome = outcome;
        const auto error = RunCapture(&probe, /*swift*/ false);
        EXPECT_FALSE(error.IsOK());
        EXPECT_FALSE(probe.PrematureAcknowledgement);
        EXPECT_EQ(probe.CaptureAttempts, 1);
        EXPECT_EQ(probe.FailureTimestamp, 4'000u);
        EXPECT_EQ(probe.Captured, outcome != ECommitOutcome::Failure);
        EXPECT_EQ(probe.Acknowledged, outcome != ECommitOutcome::Failure ? 1 : 0);
        if (outcome != ECommitOutcome::Failure) {
            EXPECT_THAT(ToString(error), HasSubstr("Finished capture test"));
        }
    }
}

TEST(TFailedMessageCaptureTest, DisabledHandlerDoesNotAcknowledgeOrWrite)
{
    TCaptureProbe probe;
    const auto error = RunCapture(&probe, /*swift*/ false, /*enabled*/ false);
    EXPECT_THAT(ToString(error), HasSubstr("Failure handler is disabled"));
    EXPECT_EQ(probe.CaptureAttempts, 0);
    EXPECT_EQ(probe.Acknowledged, 0);
}

TEST(TFailedMessageCaptureTest, SwiftRejectsCaptureRegardlessOfPipelineFlag)
{
    for (bool enabled : {false, true}) {
        TCaptureProbe probe;
        const auto error = RunCapture(&probe, /*swift*/ true, enabled);
        EXPECT_THAT(ToString(error), HasSubstr("supported only by transform computations"));
        EXPECT_EQ(probe.CaptureAttempts, 0);
        EXPECT_EQ(probe.Acknowledged, 0);
    }
}

TEST(TFailedMessageCaptureTest, PipelineSpecDefaultsToDisabled)
{
    EXPECT_FALSE(New<TPipelineSpec>()->FailureHandler->Enabled);
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf("{failure_handler={enabled=%true;};}"));
    EXPECT_TRUE(spec->FailureHandler->Enabled);
}

} // namespace
} // namespace NYT::NFlow
