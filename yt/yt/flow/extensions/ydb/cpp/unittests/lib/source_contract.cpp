#include "source_contract.h"

#include <yt/yt/flow/library/cpp/common/registry.h>

#include <yt/yt/flow/library/cpp/common/unittests/mock/source_context.h>

#include <yt/yt/flow/library/cpp/connectors/common/ordered_source_base.h>
#include <yt/yt/flow/library/cpp/connectors/common/source_base.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/framing/packer.h>

#include <util/stream/file.h>
#include <util/stream/str.h>

namespace NYT::NFlow::NYdbTesting {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TIncreasingTimeProvider
    : public ITimeProvider
{
    mutable std::atomic<TUniqueSeqNo::TUnderlying> CurrentSeqNo = 1ULL << 63;

    TFuture<TGlobalUniqueSeqNo> GenerateGlobalUniqueSeqNo() const override
    {
        return MakeFuture(TGlobalUniqueSeqNo{
            .Timestamp = TSystemTimestamp(TInstant::Now().Seconds()),
            .UniqueSeqNo = TUniqueSeqNo{CurrentSeqNo.fetch_add(1)},
        });
    }

    i64 GenerateSeqNo() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<void> InsertSeqNoBarrier() override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<TSystemTimestamp> GetTimestamp(bool /*barrier*/) const override
    {
        YT_UNIMPLEMENTED();
    }
};

IMapNodePtr MakeBoundedPartitionSpec(int limit)
{
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item("limit").Value(limit)
        .EndMap()
        ->AsMap();
    // clang-format on
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

const TStreamId SourceRunStreamId("ydb_input");

std::string PackLenvalRecord(const std::vector<std::string>& frames)
{
    TStringStream output;
    {
        NFraming::TPacker packer(NFraming::EFormat::Lenval, output);
        for (const auto& frame : frames) {
            packer.Add(frame);
        }
    }
    return output.Str();
}

TDynamicSourceContextPtr MakeDynamicSourceContext(
    const std::vector<std::pair<std::string, INodePtr>>& parameters,
    IMapNodePtr partitionSpec)
{
    auto context = New<TDynamicSourceContext>();
    context->DynamicSourceSpec = New<TDynamicSourceSpec>();
    for (const auto& [name, value] : parameters) {
        context->DynamicSourceSpec->Parameters->AddChild(name, CloneNode(value));
    }
    context->DynamicPartitionSpec = partitionSpec ? std::move(partitionSpec) : GetEphemeralNodeFactory()->CreateMap();
    return context;
}

i64 GetRecordOffset(const ISource::TMessageBatch& batch)
{
    return GetColumnValue<i64>(batch.Messages.front(), "offset");
}

////////////////////////////////////////////////////////////////////////////////

TSourceRun::TSourceRun(
    const TSourceFamily& family,
    const TDynamicSourceContextPtr& dynamicContext,
    TStateManagerMockPtr stateManager,
    bool stableReadGroups)
    : StateManager_(std::move(stateManager))
{
    auto spec = New<TSourceSpec>();
    spec->SourceClassName = family.SourceClassName;
    spec->Parameters = family.Parameters;

    auto context = CreateTestSourceContext(Invoker_);
    context->PoolInvoker = DecompressionPool_->GetInvoker();
    context->SourceSpec = spec;
    context->SourceStreamId = SourceRunStreamId;
    context->SourceKey = family.SourceKey;
    context->TimeProvider = New<TIncreasingTimeProvider>();
    context->StaticResources.emplace(YdbDriverDefaultResourceId, family.Driver);

    RunInQueue([&] {
        Source_ = TRegistry::Get()->CreateSource(context, dynamicContext);
        if (stableReadGroups) {
            DynamicPointerCast<TOrderedSourceBase>(Source_)->EnableStableReadGroups();
        }
        Source_->Init(StateManager_->CreateContext()->WithPrefix("source"));
    });
}

TSourceRun::~TSourceRun()
{
    Terminate();
}

std::vector<ISource::TMessageBatch> TSourceRun::ReadBatches(int batchCount)
{
    auto settings = New<TMessageBatcherSettings>();
    settings->MaxRowsPerBatch = TSize(1);
    settings->BatchDuration = TDuration::MilliSeconds(200);
    std::vector<ISource::TMessageBatch> batches;
    const auto deadline = TInstant::Now() + SourceRunDeadline;
    while (std::ssize(batches) < batchCount) {
        THROW_ERROR_EXCEPTION_IF(TInstant::Now() > deadline,
            "Source did not deliver the expected batches");
        auto next = RunInQueue([&] {
            return NConcurrency::WaitFor(Source_->GetNextBatch(settings)).ValueOrThrow();
        });
        for (auto& batch : next) {
            batches.push_back(std::move(batch));
        }
    }
    return batches;
}

void TSourceRun::Persist(const std::vector<ISource::TMessageBatch>& batches)
{
    RunInQueue([&] {
        for (const auto& batch : batches) {
            Source_->MarkPublished(batch.Cookie);
            Source_->MarkPersisted(batch.Cookie);
        }
        Source_->Sync();
        StateManager_->Sync();
    });
}

void TSourceRun::Terminate()
{
    if (Terminated_) {
        return;
    }
    Terminated_ = true;
    Source_->Terminate();
    RunInQueue([&] {
        Source_.Reset();
    });
    Queue_->Shutdown();
    DecompressionPool_->Shutdown();
}

const ISourcePtr& TSourceRun::GetSource() const
{
    return Source_;
}

const THoldingInvokerPtr& TSourceRun::GetInvoker() const
{
    return Invoker_;
}

////////////////////////////////////////////////////////////////////////////////

void CheckRepositionedPartitionUpdate(
    const TSourceFamily& family,
    const std::function<void(const ISourcePtr&, const TYdbPartitionInfoUpdate&)>& applyUpdate)
{
    const TString logPath = "repositioned-source-update.log";
    NLogging::TLogManager::Get()->Configure(NLogging::TLogManagerConfig::CreateLogFile(std::string(logPath)), /*sync*/ true);

    auto stateManager = New<TStateManagerMock>();
    TSourceRun run(family, MakeDynamicSourceContext(), stateManager);
    run.RunInQueue([&] {
        applyUpdate(run.GetSource(), {
                .CommittedOffsetExclusive = 5,
                .MaxOffsetExclusive = 5,
                .UpdateInstant = TInstant::Now(),
                .Repositioned = true,
                                     });
        run.GetSource()->Sync();
        stateManager->Sync();
    });
    run.Terminate();
    NLogging::TLogManager::Get()->Synchronize();
    const auto log = TFileInput(logPath).ReadAll();
    NLogging::TLogManager::Get()->Configure(NLogging::TLogManagerConfig::CreateDefault(), /*sync*/ true);

    auto state = ConvertTo<TOrderedSourcePartitionStatePtr>(stateManager->Get("/source/v0"));
    EXPECT_EQ(OffsetToInt(state->PersistedOffsetExclusive), 5);
    EXPECT_TRUE(log.Contains("Skip NextReadOffset forward to an external position"));
    EXPECT_FALSE(log.Contains("Rewind NextReadOffset"));
    EXPECT_FALSE(log.Contains("Rewind PersistedOffsetExclusive"));
}

void CheckRejectedPartitionSpec(
    const TSourceFamily& family,
    const std::function<std::pair<int, std::string>(const ISourcePtr&)>& readEffectiveOptions)
{
    auto accepted = MakeDynamicSourceContext(
        {{"malformed_message_policy", ConvertToNode("keep")}},
        MakeBoundedPartitionSpec(3));
    TSourceRun run(family, accepted, New<TStateManagerMock>());
    auto source = DynamicPointerCast<TSourceBase>(run.GetSource());
    run.GetInvoker()->StartHolding();
    EXPECT_THROW(
        source->Reconfigure(MakeDynamicSourceContext(
            {{"malformed_message_policy", ConvertToNode("drop")}},
            MakeBoundedPartitionSpec(0))),
        std::exception);
    run.GetInvoker()->StopHolding();

    EXPECT_EQ(run.GetInvoker()->GetHeldCount(), 0);
    EXPECT_EQ(source->GetDynamicContext(), accepted);
    EXPECT_EQ(readEffectiveOptions(run.GetSource()), std::pair(3, std::string("Keep")));
}

void CheckKeptMalformedPayload(const TSourceFamily& family)
{
    TSourceRun run(
        family,
        MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("keep")}}),
        New<TStateManagerMock>());
    auto batches = run.ReadBatches(1);
    ASSERT_EQ(batches.front().Messages.size(), 1u);
    EXPECT_EQ(
        family.ReadUnparsedData(GetColumnValue<TYsonString>(batches.front().Messages.front(), "unparsed")),
        MalformedLenvalPayload);
}

void CheckQueuedSourceReconfigurations(const TSourceFamily& family)
{
    TSourceRun run(
        family,
        MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("fail")}}),
        New<TStateManagerMock>());
    EXPECT_EQ(GetRecordOffset(run.ReadBatches(1).front()), 0);

    auto source = DynamicPointerCast<TSourceBase>(run.GetSource());
    auto contextB = MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("drop")}});
    run.GetInvoker()->StartHolding();
    source->Reconfigure(MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("keep")}}));
    source->Reconfigure(contextB);
    EXPECT_THROW(
        source->Reconfigure(MakeDynamicSourceContext({{"malformed_message_policy", ConvertToNode("invalid")}})),
        std::exception);
    run.GetInvoker()->StopHolding();
    ASSERT_EQ(run.GetInvoker()->GetHeldCount(), 2);
    EXPECT_EQ(source->GetDynamicContext(), contextB);

    run.GetInvoker()->ReleaseNext();
    auto underA = run.ReadBatches(1);
    EXPECT_EQ(GetRecordOffset(underA.front()), 1);
    EXPECT_EQ(
        family.ReadUnparsedData(GetColumnValue<TYsonString>(underA.front().Messages.front(), "unparsed")),
        MalformedLenvalPayload);

    run.GetInvoker()->ReleaseNext();
    auto underB = run.ReadBatches(1);
    EXPECT_EQ(GetRecordOffset(underB.front()), 3);
    EXPECT_EQ(GetColumnValue<std::string>(underB.front().Messages.front(), "data"), "after");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting
