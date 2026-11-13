#include "sink.h"

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/computation_controller.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/cache/cache.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NFlow {

using namespace NApi;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

IClientPtr GetExportClient(const auto& context, const TStaticTableExportParametersPtr& parameters)
{
    const auto cluster = parameters->OutputDirectory.GetCluster();
    return cluster ? context->ClientsCache->GetClient(*cluster) : context->GetClient();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ValidateStaticTableExportComputation(
    const TComputationSpec& spec,
    const TStaticTableExportParametersPtr& parameters,
    const std::string& sinkClassName,
    const std::string& writeTimeColumn)
{
    THROW_ERROR_EXCEPTION_UNLESS(spec.SourceStreams.size() == 1 && spec.OutputStreamIds.size() == 1 && spec.Sinks.size() == 1,
        "Static table export requires one source, one output stream and one export sink");
    const auto& sink = spec.Sinks.begin()->second;
    THROW_ERROR_EXCEPTION_UNLESS(sink->SinkClassName == sinkClassName && sink->InputStreamIds == spec.OutputStreamIds,
        "Static table export requires its sink on the output stream");

    const auto& strategy = spec.WatermarkStrategy;
    const auto& assigner = strategy->EventTimestampAssigner;
    THROW_ERROR_EXCEPTION_UNLESS(assigner && assigner->Column == writeTimeColumn &&
            assigner->Format == ETimestampFormat::MilliSeconds && !assigner->LimitBySystemTimestamp,
        "Static table export requires event timestamps from \"write_time_column\" in milliseconds");
    const auto& generator = strategy->WatermarkGenerator;
    THROW_ERROR_EXCEPTION_UNLESS(generator && !generator->UseSourceWatermark && !generator->LateDataPartitions &&
            generator->OutOfOrdernessBound == TDuration::Zero() &&
            strategy->WatermarkPercentile->Value == PreciseWatermarkPercentile,
        "Static table export requires precise generated event watermarks without out-of-orderness");
    // Window closure follows the watermark, so every idle partition must release it.
    THROW_ERROR_EXCEPTION_UNLESS(generator->IdlePartitions && generator->IdlePartitions->MaxRatio == 1.0,
        "Static table export requires \"idle_partitions\" with \"max_ratio\" 1.0");
    const auto& alignment = strategy->WatermarkAlignment;
    THROW_ERROR_EXCEPTION_UNLESS(alignment && alignment->DriftBound > parameters->WindowPeriod + parameters->ClosureDelta,
        "Static table export requires \"drift_bound\" above \"window_period\" + \"closure_delta\"");
    ValidateStaticTableExportBand(parameters, alignment->DriftBound);
    const auto* resource = spec.RequiredResourceIds.FindPtr(parameters->WriterResourceId);
    THROW_ERROR_EXCEPTION_UNLESS(resource && (*resource)->Worker && !(*resource)->Controller,
        "Static table export requires \"writer_resource_id\" among worker-only \"required_resource_ids\"");
}

INodePtr MakeStaticTableExportOwner(
    const NYPath::TYPath& pipelinePath,
    const TComputationId& computationId,
    const TStaticTableExportParametersPtr& parameters,
    const IMapNodePtr& source)
{
    // clang-format off
    auto owner = BuildYsonNodeFluently()
        .BeginMap()
            .Item("pipeline_path").Value(pipelinePath)
            .Item("computation_id").Value(computationId)
            .Item("window_period").Value(parameters->WindowPeriod)
            .Item("max_unpublished_window_count").Value(parameters->MaxUnpublishedWindowCount)
            .Item("table_name_utc_offset").Value(parameters->TableNameUtcOffset)
        .EndMap()
        ->AsMap();
    // clang-format on
    for (const auto& [key, child] : source->GetChildren()) {
        owner->AddChild(key, CloneNode(child));
    }
    return owner;
}

IListNodePtr BuildStaticTableExportPartitions(std::span<const TStaticTableExportRange> ranges)
{
    auto partitions = GetEphemeralNodeFactory()->CreateList();
    for (const auto& range : ranges) {
        // clang-format off
        partitions->AddChild(BuildYsonNodeFluently()
            .BeginMap()
                .Item("partition").Value(range.Partition)
                .Item("first_offset").Value(range.FirstOffset)
                .Item("next_offset").Value(range.NextOffset)
            .EndMap());
        // clang-format on
    }
    return partitions;
}

////////////////////////////////////////////////////////////////////////////////

TStaticTableExportSinkBase::TStaticTableExportSinkBase(TSinkContextPtr context, TDynamicSinkContextPtr dynamicContext, int partition)
    : TSinkBase(std::move(context), std::move(dynamicContext))
    , Partition_(partition)
{
    SubscribeReconfigured(BIND([this] (const TDynamicSinkContextPtr& /*dynamicContext*/) {
        if (Participant_) {
            Participant_->SetDraining(IsFinishing());
        }
    }));
}

void TStaticTableExportSinkBase::ValidateDynamicSpec() const
{ }

bool TStaticTableExportSinkBase::IsDraining() const
{
    return GetDynamicContext()->DynamicComputationSpec->Draining;
}

bool TStaticTableExportSinkBase::IsFinishing() const
{
    // A completing job waits for the publication like a drain, so the writer refreshes the progress for it too.
    return IsDraining() || GetContext()->Partition->State == EPartitionState::Completing;
}

void TStaticTableExportSinkBase::Init(IInitContextPtr /*initContext*/)
{
    ValidateDynamicSpec();
    const auto& parameters = GetParameters();
    auto binding = MakeBinding();
    binding.Client = GetExportClient(GetContext(), parameters);
    binding.Parameters = parameters;
    const auto resource = GetContext()->GetStaticResource(parameters->WriterResourceId)->As<TStaticTableExportWriterResource>();
    Participant_ = resource->RegisterPartition(
        std::move(binding),
        {.Partition = Partition_, .Generation = GetContext()->Job->Generation},
        GetContext()->PoolInvoker);
    Participant_->SetDraining(IsFinishing());
}

void TStaticTableExportSinkBase::Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed)
{
    Participant_->Enqueue(
        GetExportMessage(*message),
        message,
        TOnDistributedCallback::FromCallback([weakThis = MakeWeak(this), streamId = message->StreamId, timestamp = message->EventTimestamp, onDistributed = std::move(onDistributed)] () mutable {
            if (auto this_ = weakThis.Lock()) {
                this_->ObserveEventLag(streamId, timestamp);
            }
            onDistributed();
        }));
}

void TStaticTableExportSinkBase::Sync(IDynamicTableTransactionPtr /*transaction*/)
{
    ValidateDynamicSpec();
    Participant_->ThrowOnError();
}

void TStaticTableExportSinkBase::Commit()
{ }

THashMap<TStreamId, TInflightStreamTraverseDataPtr> TStaticTableExportSinkBase::BuildInflight() const
{
    // A completing job waits for publication too: windows close only once the finished source stream completes,
    // so the executing job of a finite partition must finish first.
    if (!IsFinishing() || !Participant_) {
        return {};
    }
    // A drain completes only after the windows this partition wrote are published, which is unknown before the claim.
    const auto state = Participant_->GetState();
    auto inflight = New<TInflightStreamTraverseData>();
    // A finished job stops reporting, so a completing one waits for the bootstrap its silence may be needed for.
    const bool known = IsDraining() ? state.Claimed : state.Bootstrapped;
    inflight->Empty = known && state.PendingMessageCount == 0 && !state.Unpublished;
    return {{*GetSpec()->InputStreamIds.begin(), std::move(inflight)}};
}

IMapNodePtr TStaticTableExportSinkBase::GetPartitionStatus() const
{
    if (!Participant_) {
        return nullptr;
    }
    const auto state = Participant_->GetState();
    // clang-format off
    return BuildYsonNodeFluently()
        .BeginMap()
            .Item("drained").Value(IsDraining() && state.Claimed && state.PendingMessageCount == 0)
            .Item("silent_while_draining").Value(state.SilentWhileDraining)
            .Item("silent_while_completing").Value(state.SilentWhileDraining && !IsDraining() && IsFinishing())
            .OptionalItem("first_write_time", state.FirstWriteTimestamp ? std::optional(state.FirstWriteTimestamp->Underlying()) : std::nullopt)
        .EndMap()
        ->AsMap();
    // clang-format on
}

void TStaticTableExportSinkBase::BeforeGracefulShutdown()
{
    YT_VERIFY(Participant_);
    // Committing the admitted prefix now lets the successor claim the partition without fencing it.
    NConcurrency::WaitFor(Participant_->PrepareForHandoff())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

void TStaticTableExportSinkControllerBase::UpdatePartitionStatuses(const TSinkPartitionStatuses& statuses)
{
    PartitionStatuses_ = statuses;
}

std::optional<i64> TStaticTableExportSinkControllerBase::GetReceiverChannelCount()
{
    return std::nullopt;
}

void TStaticTableExportSinkControllerBase::UpdatePublication(const TExportSource& source)
{
    const auto parameters = GetParameters();
    if (!Publisher_) {
        auto storage = New<TStaticTableExportWriterStorage>(
            GetExportClient(GetContext(), parameters),
            parameters,
            GetExportSchema(),
            MakeOwner(),
            source.Identity,
            /*tableWriterConfig*/ nullptr,
            /*tableWriterCount*/ 1,
            GetContext()->Profiler);
        Publisher_ = New<TStaticTableExportPublisher>(
            std::move(storage),
            parameters,
            [weakThis = MakeWeak(this), parameters] (ui64 window, std::span<const TStaticTableExportRange> ranges) {
                auto this_ = weakThis.Lock();
                THROW_ERROR_EXCEPTION_UNLESS(this_, "Static table export sink controller is destroyed");
                return TStaticTableExportPublication{
                    .Name = GetStaticTableExportTableName(window, parameters->TableNameUtcOffsetSeconds),
                    .Attributes = this_->BuildPublicationAttributes(ranges),
                };
            },
            GetContext()->Invoker,
            GetContext()->StatusProfiler->ErrorState("/publish"),
            Logger,
            GetContext()->Profiler);
    }

    int drained = 0;
    int firstWriteReportCount = 0;
    int silentDrainingCount = 0;
    int silentCompletingCount = 0;
    std::optional<TSystemTimestamp> firstWriteTimestamp;
    for (const auto& [key, status] : PartitionStatuses_) {
        const auto partition = FindPartition(key, source.Identity);
        if (!partition || *partition >= source.PartitionCount || !status.Status) {
            continue;
        }
        drained += status.Status->FindChildValue<bool>("drained") == true;
        silentDrainingCount += status.Status->FindChildValue<bool>("silent_while_draining") == true;
        silentCompletingCount += status.Status->FindChildValue<bool>("silent_while_completing") == true;
        if (const auto firstWriteTime = status.Status->FindChildValue<ui64>("first_write_time")) {
            ++firstWriteReportCount;
            firstWriteTimestamp = std::min(firstWriteTimestamp.value_or(TSystemTimestamp(*firstWriteTime)), TSystemTimestamp(*firstWriteTime));
        }
    }
    const auto watermarkState = GetWatermarkState();
    Publisher_->Update({
        .PartitionCount = source.PartitionCount,
        .EventWatermark = watermarkState ? watermarkState->GetEventWatermark(*GetSpec()->InputStreamIds.begin()) : TSystemTimestamp(),
        .AllDrained = drained == source.PartitionCount,
        .FirstWriteTimestamp = firstWriteTimestamp,
        .FirstWriteReportCount = firstWriteReportCount,
        .SilentDrainingCount = silentDrainingCount,
        .SilentCompletingCount = silentCompletingCount,
        .Dynamic = GetDynamicParameters(),
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
