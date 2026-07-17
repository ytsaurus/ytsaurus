#include "flow_view.h"

#include "private.h"

#include "checksum.h"

#include <yt/yt/flow/library/cpp/misc/version_helpers.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <yt/yt/core/yson/forwarding_consumer.h>
#include <yt/yt/core/yson/null_consumer.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <library/cpp/yt/misc/variant.h>

#include <util/digest/multi.h>
#include <util/generic/map.h>
#include <vector>

namespace NYT::NFlow {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const TPersistedStateName FlowViewLayoutPartitionsStateName = "layout_partitions";
static const TPersistedStateName FlowViewLayoutJobsStateName = "layout_jobs";
static const TPersistedStateName FlowViewLayoutWorkerSpecsStateName = "layout_worker_specs";
constinit const auto& Logger = FlowStateLogger;

// Above a single partition's job status, so partitions stay unparsed while the big maps are navigable.
static constexpr i64 FlowViewHybridLeafSizeThreshold = 64 * 1024;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Streams |ysonStruct| as a map, but for the top-level key |childKey| emits |writeChild(consumer)|
// instead of the struct's own value. Lets the flow view render its layout as "partitions"/"jobs"
// while streaming (no node tree) and without changing the struct's default (persisted) serialization.
class TChildOverridingConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    TChildOverridingConsumer(
        NYson::IYsonConsumer* underlying,
        TStringBuf childKey,
        std::function<void(NYson::IYsonConsumer*)> writeChild)
        : Underlying_(underlying)
        , ChildKey_(childKey)
        , WriteChild_(std::move(writeChild))
    { }

private:
    NYson::IYsonConsumer* const Underlying_;
    const std::string ChildKey_;
    const std::function<void(NYson::IYsonConsumer*)> WriteChild_;
    // Map-nesting depth; an attribute block adds 2, so attribute keys never sit at the struct's top
    // level (Depth 1) where the child override matches.
    int Depth_ = 0;

    void OnMyStringScalar(TStringBuf value) override
    {
        Underlying_->OnStringScalar(value);
    }

    void OnMyInt64Scalar(i64 value) override
    {
        Underlying_->OnInt64Scalar(value);
    }

    void OnMyUint64Scalar(ui64 value) override
    {
        Underlying_->OnUint64Scalar(value);
    }

    void OnMyDoubleScalar(double value) override
    {
        Underlying_->OnDoubleScalar(value);
    }

    void OnMyBooleanScalar(bool value) override
    {
        Underlying_->OnBooleanScalar(value);
    }

    void OnMyEntity() override
    {
        Underlying_->OnEntity();
    }

    void OnMyBeginList() override
    {
        Underlying_->OnBeginList();
    }

    void OnMyListItem() override
    {
        Underlying_->OnListItem();
    }

    void OnMyEndList() override
    {
        Underlying_->OnEndList();
    }

    void OnMyBeginMap() override
    {
        ++Depth_;
        Underlying_->OnBeginMap();
    }

    void OnMyEndMap() override
    {
        --Depth_;
        Underlying_->OnEndMap();
    }

    void OnMyBeginAttributes() override
    {
        Depth_ += 2;
        Underlying_->OnBeginAttributes();
    }

    void OnMyEndAttributes() override
    {
        Depth_ -= 2;
        Underlying_->OnEndAttributes();
    }

    void OnMyKeyedItem(TStringBuf key) override
    {
        Underlying_->OnKeyedItem(key);
        if (Depth_ == 1 && key == ChildKey_) {
            // Drop the struct's own (empty) value for this key and emit the override instead.
            Forward(NYson::GetNullYsonConsumer(), [this] {
                WriteChild_(Underlying_);
            },
                NYson::EYsonType::Node);
        }
    }
};

void SerializeStructOverridingChild(
    const NYTree::TYsonStructBase& ysonStruct,
    TStringBuf childKey,
    const std::function<void(NYson::IYsonConsumer*)>& writeChild,
    NYson::IYsonConsumer* consumer)
{
    TChildOverridingConsumer overridingConsumer(consumer, childKey, writeChild);
    Serialize(ysonStruct, &overridingConsumer);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartition::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_id", &TThis::PartitionId);

    registrar.Parameter("computation_id", &TThis::ComputationId);

    registrar.Parameter("lower_key", &TThis::LowerKey)
        .Default();
    registrar.Parameter("upper_key", &TThis::UpperKey)
        .Default();
    registrar.Parameter("source_key", &TThis::SourceKey)
        .Default();

    registrar.Parameter("current_job_id", &TThis::CurrentJobId)
        .Default();

    registrar.Parameter("state", &TThis::State)
        .Default(EPartitionState::Executing);

    registrar.Parameter("state_epoch", &TThis::StateEpoch);

    registrar.Parameter("state_timestamp", &TThis::StateTimestamp);
}

bool TPartition::ContainsKey(const TKey& key) const
{
    if (LowerKey && UpperKey) {
        return TKeyRange{*LowerKey, *UpperKey}.Contains(key);
    }
    if (SourceKey) {
        return *SourceKey == key;
    }
    return false;
}

bool TPartition::IsWorking() const
{
    return State == EPartitionState::Executing ||
        State == EPartitionState::Completing ||
        State == EPartitionState::Interrupting;
}

////////////////////////////////////////////////////////////////////////////////

void TJob::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);

    registrar.Parameter("worker_address", &TThis::WorkerAddress);
    registrar.Parameter("worker_incarnation_id", &TThis::WorkerIncarnationId)
        .Default();

    registrar.Parameter("partition_id", &TThis::PartitionId);

    registrar.Parameter("lease_id", &TThis::LeaseId);
}

////////////////////////////////////////////////////////////////////////////////

void TNodePerformanceMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("cpu_usage_current", &TThis::CpuUsageCurrent)
        .Default();
    registrar.Parameter("cpu_usage_30s", &TThis::CpuUsage30s)
        .Default();
    registrar.Parameter("cpu_usage_10m", &TThis::CpuUsage10m)
        .Default();
    registrar.Parameter("memory_usage_current", &TThis::MemoryUsageCurrent)
        .Default();
    registrar.Parameter("memory_usage_30s", &TThis::MemoryUsage30s)
        .Default();
    registrar.Parameter("memory_usage_10m", &TThis::MemoryUsage10m)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TAggregatedNodePerformanceMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("total", &TThis::Total)
        .DefaultNew();
    registrar.Parameter("avg", &TThis::Avg)
        .DefaultNew();
    registrar.Parameter("max", &TThis::Max)
        .DefaultNew();
}

template <typename TAggregator>
static void NodePerformanceMetricsApply(
    const TNodePerformanceMetricsPtr& result,
    const TNodePerformanceMetricsPtr& metrics,
    TAggregator&& aggregator)
{
    result->CpuUsageCurrent = aggregator(result->CpuUsageCurrent.value_or(0), metrics->CpuUsageCurrent.value_or(0));
    result->CpuUsage30s = aggregator(result->CpuUsage30s.value_or(0), metrics->CpuUsage30s.value_or(0));
    result->CpuUsage10m = aggregator(result->CpuUsage10m.value_or(0), metrics->CpuUsage10m.value_or(0));
    result->MemoryUsage10m = aggregator(result->MemoryUsage10m, metrics->MemoryUsage10m);
    result->MemoryUsage30s = aggregator(result->MemoryUsage30s, metrics->MemoryUsage30s);
    result->MemoryUsageCurrent = aggregator(result->MemoryUsageCurrent, metrics->MemoryUsageCurrent);
}

template <typename TAggregator>
static void AggregateNodePerformanceMetrics(
    const TNodePerformanceMetricsPtr& result,
    const std::vector<TNodePerformanceMetricsPtr>& metrics,
    TAggregator&& aggregator)
{
    for (const auto& metricsElement : metrics) {
        NodePerformanceMetricsApply(result, metricsElement, aggregator);
    }
}

void NodePerformanceMetricsAdd(const TNodePerformanceMetricsPtr& result, const TNodePerformanceMetricsPtr& metrics)
{
    NodePerformanceMetricsApply(result, metrics, std::plus<>());
}

TAggregatedNodePerformanceMetricsPtr AggregateNodePerformanceMetrics(const std::vector<TNodePerformanceMetricsPtr>& metrics)
{
    auto result = New<TAggregatedNodePerformanceMetrics>();
    AggregateNodePerformanceMetrics(result->Total, metrics, std::plus<>());
    AggregateNodePerformanceMetrics(result->Max, metrics, [] (auto a, auto b) {
        return std::max(a, b);
    });

    {
        auto count = metrics.size();
        result->Avg->CpuUsageCurrent = result->Total->CpuUsageCurrent.value_or(0) / count;
        result->Avg->CpuUsage30s = result->Total->CpuUsage30s.value_or(0) / count;
        result->Avg->CpuUsage10m = result->Total->CpuUsage10m.value_or(0) / count;
        result->Avg->MemoryUsage10m = result->Total->MemoryUsage10m / count;
        result->Avg->MemoryUsage30s = result->Total->MemoryUsage30s / count;
        result->Avg->MemoryUsageCurrent = result->Total->MemoryUsageCurrent / count;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TNodeInputStreamMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("messages_per_second", &TThis::MessagesPerSecond)
        .Default();
    registrar.Parameter("bytes_per_second", &TThis::BytesPerSecond)
        .Default();
    registrar.Parameter("heavy_hitters", &TThis::HeavyHitters)
        .Default();
    registrar.Parameter("pivots", &TThis::Pivots)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TAggregatedNodeInputStreamMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("total", &TThis::Total)
        .Default();
    registrar.Parameter("avg", &TThis::Avg)
        .Default();
    registrar.Parameter("max", &TThis::Max)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TAggregatedNodeInputStreamMetricsPtr AggregateNodeInputStreamMetrics(const std::vector<TNodeInputStreamMetrics>& metrics)
{
    auto result = New<TAggregatedNodeInputStreamMetrics>();

    for (const auto& metric : metrics) {
        result->Total.MessagesPerSecond += metric.MessagesPerSecond;
        result->Total.BytesPerSecond += metric.BytesPerSecond;
        result->Max.MessagesPerSecond = std::max(result->Max.MessagesPerSecond, metric.MessagesPerSecond);
        result->Max.BytesPerSecond = std::max(result->Max.BytesPerSecond, metric.BytesPerSecond);
        for (const auto& [part, key] : metric.HeavyHitters) {
            result->Total.HeavyHitters.emplace_back(part * metric.MessagesPerSecond, key);
        }
    }
    for (auto& [part, key] : result->Total.HeavyHitters) {
        part /= result->Total.MessagesPerSecond;
    }
    result->Avg.MessagesPerSecond = result->Total.MessagesPerSecond / metrics.size();
    result->Avg.BytesPerSecond = result->Total.BytesPerSecond / metrics.size();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TNodeInputMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("global", &TThis::Global)
        .Default();
    registrar.Parameter("streams", &TThis::Streams)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TAggregatedNodeInputMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("global", &TThis::Global)
        .Default();
    registrar.Parameter("streams", &TThis::Streams)
        .Default();
}

TAggregatedNodeInputMetricsPtr AggregateNodeInputMetrics(const std::vector<TNodeInputMetricsPtr>& metrics)
{
    auto result = New<TAggregatedNodeInputMetrics>();
    std::vector<TNodeInputStreamMetrics> globals;
    THashMap<TStreamId, std::vector<TNodeInputStreamMetrics>> streams;
    for (const auto& metric : metrics) {
        globals.push_back(metric->Global);
        for (const auto& [streamId, stream] : metric->Streams) {
            streams[streamId].push_back(stream);
        }
    }
    result->Global = AggregateNodeInputStreamMetrics(globals);
    for (const auto& [streamId, metrics] : streams) {
        result->Streams[streamId] = AggregateNodeInputStreamMetrics(metrics);
    }
    return result;
}

THashMap<TComputationId, TAggregatedNodeInputMetricsPtr> AggregateInputMetricsByComputation(const TFlowViewPtr& flowView)
{
    const auto& flowLayout = flowView->State->ExecutionSpec->Layout;
    THashMap<TComputationId, std::vector<TNodeInputMetricsPtr>> groupedMetrics;
    for (const auto& [partitionId, partition] : flowLayout->Partitions) {
        if (partition->State == EPartitionState::Executing || partition->State == EPartitionState::Completing || partition->State == EPartitionState::Interrupting) {
            auto statusIt = flowView->Feedback->PartitionJobStatuses.find(partitionId);
            if (statusIt == flowView->Feedback->PartitionJobStatuses.end()) {
                continue;
            }
            const auto& status = statusIt->second;
            if (!status->CurrentJobStatus) {
                continue;
            }
            groupedMetrics[partition->ComputationId].push_back(status->CurrentJobStatus->InputMetrics);
        }
    }
    THashMap<TComputationId, TAggregatedNodeInputMetricsPtr> result;
    for (const auto& [computationId, metrics] : groupedMetrics) {
        result[computationId] = AggregateNodeInputMetrics(metrics);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TJobEntityLimitStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("limit", &TThis::Limit)
        .Default(0);
    registrar.Parameter("used", &TThis::Used)
        .Default(0);
    registrar.Parameter("pending", &TThis::Pending)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TJobStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("job_id", &TThis::JobId);

    registrar.Parameter("is_finished", &TThis::IsFinished)
        .Default(false);

    registrar.Parameter("error", &TThis::Error)
        .Default();

    registrar.Parameter("retryable_errors", &TThis::RetryableErrors)
        .Default();

    registrar.Parameter("start_time", &TThis::StartTime)
        .Default();

    registrar.Parameter("finish_time", &TThis::FinishTime)
        .Default();

    registrar.Parameter("update_time", &TThis::UpdateTime)
        .Default();

    registrar.Parameter("inited_time", &TThis::InitedTime)
        .Default();

    registrar.Parameter("epoch", &TThis::Epoch)
        .Default(0);

    registrar.Parameter("from_partition_traverse_data", &TThis::FromPartitionTraverseData)
        .Default();

    registrar.Parameter("performance_metrics", &TThis::PerformanceMetrics)
        .DefaultNew();

    registrar.Parameter("input_metrics", &TThis::InputMetrics)
        .DefaultNew();

    registrar.Parameter("partition_status", &TThis::PartitionStatus)
        .Default();

    registrar.Parameter("epoch_part_times", &TThis::EpochPartTimes)
        .DontSerializeDefault()
        .Default();

    registrar.Parameter("input_limits", &TThis::InputLimits)
        .DontSerializeDefault()
        .Default();

    registrar.Parameter("output_limits", &TThis::OutputLimits)
        .DontSerializeDefault()
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMessageDistributorStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("stream_timestamp_statistics", &TThis::StreamTimestampStatistics)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TWorkerResourceStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("queue_size_30s", &TThis::QueueSize30s)
        .Default();
    registrar.Parameter("queue_size_10m", &TThis::QueueSize10m)
        .Default();
    registrar.Parameter("queue_growth_rate_30s", &TThis::QueueGrowthRate30s)
        .Default();
    registrar.Parameter("queue_growth_rate_10m", &TThis::QueueGrowthRate10m)
        .Default();
    registrar.Parameter("queue_push_rate_30s", &TThis::QueuePushRate30s)
        .Default();
    registrar.Parameter("queue_push_rate_10m", &TThis::QueuePushRate10m)
        .Default();
    registrar.Parameter("queue_fetch_rate_30s", &TThis::QueueFetchRate30s)
        .Default();
    registrar.Parameter("queue_fetch_rate_10m", &TThis::QueueFetchRate10m)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TWorkerStatus::Register(TRegistrar registrar)
{
    registrar.Parameter("previous_crash_error", &TThis::PreviousCrashError)
        .Default();
    registrar.Parameter("errors", &TThis::Errors)
        .Default();
    registrar.Parameter("message_distributor_status", &TThis::MessageDistributorStatus)
        .DefaultNew();
    registrar.Parameter("resource_statuses", &TThis::ResourceStatuses)
        .Default();
    registrar.Parameter("preloaded_resource_states", &TThis::PreloadedResourceStates)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TWorkerSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("preload_resources", &TThis::PreloadResources)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFlowLayout::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TFlowLayout* layout) {
        layout->SetAsSlave();
    });
};

TVersion TFlowLayout::GetVersion() const
{
    // Any state can be used.
    return TVersion{CommittedPartitions_->GetControlSequenceId().Underlying()};
};

template <class TKey, class TValue>
void CreateStateCommon(TPersistedStatePtr<TKey, TValue>& state, TPersistedStateControlPtr<std::string>& control, const TPersistedStateName& name)
{
    state = control->CreateState<TKey, TValue>(name, TCommonSerializer<TKey, TValue>{});
}

void TFlowLayout::AttachToControl(TPersistedStateControlPtr<std::string> control)
{
    PersistedStateControl_ = std::move(control);
    CreateStateCommon(CommittedPartitions_, PersistedStateControl_, FlowViewLayoutPartitionsStateName);
    CreateStateCommon(CommittedJobs_, PersistedStateControl_, FlowViewLayoutJobsStateName);
    CreateStateCommon(CommittedWorkerSpecs_, PersistedStateControl_, FlowViewLayoutWorkerSpecsStateName);
    Partitions.SetState(CommittedPartitions_);
    Jobs.SetState(CommittedJobs_);
    WorkerSpecs.SetState(CommittedWorkerSpecs_);
}

void TFlowLayout::SetAsSlave()
{
    AttachToControl(New<TPersistedStateControl<std::string>>());
}

TFlowLayoutPtr TFlowLayout::Clone() const
{
    TFlowLayoutPtr result = New<TFlowLayout>();
    result->PersistedStateControl_ = PersistedStateControl_;
    result->CommittedPartitions_ = CommittedPartitions_;
    result->CommittedJobs_ = CommittedJobs_;
    result->CommittedWorkerSpecs_ = CommittedWorkerSpecs_;
    result->Partitions.SetState(result->CommittedPartitions_);
    result->Jobs.SetState(result->CommittedJobs_);
    result->WorkerSpecs.SetState(result->CommittedWorkerSpecs_);
    // Remember the source's live (write) transaction so a later CreateSnapshot(/*committed*/ false)
    // can seed the snapshot with its still-uncommitted changes. Null when cloned outside a mutation.
    result->SourceWriteTransaction_ = Txn_;
    return result;
}

void TFlowLayout::SerializeAsYson(NYson::IYsonConsumer* consumer)
{
    // Non-const because the transactional Partitions/Jobs expose only non-const iteration; this pass
    // runs on the committed flow-view snapshot, so it is read-only in practice.
    consumer->OnBeginMap();
    consumer->OnKeyedItem("partitions");
    consumer->OnBeginMap();
    for (const auto& [key, value] : Partitions) {
        consumer->OnKeyedItem(ToString(key));
        Serialize(value, consumer);
    }
    consumer->OnEndMap();
    consumer->OnKeyedItem("jobs");
    consumer->OnBeginMap();
    for (const auto& [key, value] : Jobs) {
        consumer->OnKeyedItem(ToString(key));
        Serialize(value, consumer);
    }
    consumer->OnEndMap();
    consumer->OnEndMap();
}

void TFlowLayout::Apply(const std::vector<TPersistedStateStorageRow<std::string>>& rows)
{
    PersistedStateControl_->Apply(rows, nullptr);
}

std::vector<TPersistedStateStorageRow<std::string>> TFlowLayout::Follow(TSequenceId from) const
{
    return PersistedStateControl_->Follow(from);
}

void TFlowLayout::StartMutation(TFlowStateMutationNotifierPtr notifier)
{
    YT_ASSERT(MutationNotifier_ == nullptr);
    MutationNotifier_ = std::move(notifier);
    Txn_ = PersistedStateControl_->StartTransaction();
    Jobs.SetTransaction(Txn_);
    Partitions.SetTransaction(Txn_);
    WorkerSpecs.SetTransaction(Txn_);
    Updated_ = 0;
}

void TFlowLayout::CreateSnapshot(bool committed)
{
    YT_ASSERT(MutationNotifier_ == nullptr);
    if (!committed && SourceWriteTransaction_) {
        // Seed the read-only snapshot with the source's uncommitted writes so the snapshot is
        // consistent with the in-flight mutation (committed base + uncommitted write set).
        Txn_ = PersistedStateControl_->StartTransaction(/*readOnly*/ true, /*copyWritesFrom*/ SourceWriteTransaction_);
    } else {
        Txn_ = PersistedStateControl_->StartTransaction(/*readOnly*/ true);
    }
    SourceWriteTransaction_ = nullptr;
    Jobs.SetTransaction(Txn_);
    Partitions.SetTransaction(Txn_);
    WorkerSpecs.SetTransaction(Txn_);
    Updated_ = 0;
}

void TFlowLayout::CommitMutation(TPersistedStateCommitContext* context)
{
    Txn_->Commit(context);
    Txn_ = nullptr;
    Jobs.SetTransaction(Txn_);
    Partitions.SetTransaction(Txn_);
    WorkerSpecs.SetTransaction(Txn_);
    MutationNotifier_ = nullptr;
}

void TFlowLayout::CreatePartition(TPartitionPtr partition)
{
    Y_ENSURE(partition->PartitionId);
    Y_ENSURE(!partition->ComputationId.Underlying().empty());
    Y_ENSURE(partition->StateTimestamp);
    Y_ENSURE(!partition->CurrentJobId);
    YT_TLOG_INFO("CreatePartition")
        .With("PartitionId", partition->PartitionId)
        .With("ComputationId", partition->ComputationId)
        .With("LowerKey", partition->LowerKey, "%Qv")
        .With("UpperKey", partition->UpperKey, "%Qv")
        .With("SourceKey", partition->SourceKey, "%Qv");
    THROW_ERROR_EXCEPTION_UNLESS(Partitions.emplace(partition->PartitionId, partition).second == true, "PartitionId duplicate");
    ++Updated_;
    if (MutationNotifier_) {
        MutationNotifier_->OnCreatePartition(partition);
    }
}

void TFlowLayout::UpdatePartition(TPartitionPtr partition)
{
    Y_ENSURE(partition->PartitionId);
    Y_ENSURE(!partition->ComputationId.Underlying().empty());
    Y_ENSURE(partition->StateEpoch);
    Y_ENSURE(partition->StateTimestamp);
    Y_ENSURE(!partition->CurrentJobId);
    auto oldPartition = GetOrCrash(Partitions, partition->PartitionId);
    if (oldPartition->CurrentJobId) {
        auto job = GetOrCrash(Jobs, *oldPartition->CurrentJobId);
        EraseOrCrash(Jobs, job->JobId);
        YT_TLOG_INFO("RemoveJob during UpdatePartition")
            .With("JobId", job->JobId)
            .With("PartitionId", partition->PartitionId)
            .With("ComputationId", partition->ComputationId);
        if (MutationNotifier_) {
            MutationNotifier_->OnRemoveJob(job, EJobFinishReason::PartitionUpdated);
        }
    }
    YT_TLOG_INFO("UpdatePartition")
        .With("PartitionId", partition->PartitionId)
        .With("OldState", oldPartition->State)
        .With("NewState", partition->State);
    Partitions.insert_or_assign(partition->PartitionId, partition);
    ++Updated_;
    if (MutationNotifier_) {
        MutationNotifier_->OnUpdatePartition(oldPartition, partition);
    }
}

void TFlowLayout::UpdatePartition(TPartitionId id, EPartitionState state, i64 stateEpoch, TInstant stateTimestamp)
{
    auto oldPartition = GetOrCrash(Partitions, id);
    auto newPartition = CloneYsonStruct(oldPartition);
    newPartition->State = state;
    newPartition->StateEpoch = stateEpoch;
    newPartition->StateTimestamp = stateTimestamp;
    newPartition->CurrentJobId.reset();
    UpdatePartition(newPartition);
}

void TFlowLayout::RemovePartition(const TPartitionId& id)
{
    auto oldPartition = GetOrCrash(Partitions, id);
    if (oldPartition->CurrentJobId) {
        auto job = GetOrCrash(Jobs, *oldPartition->CurrentJobId);
        EraseOrCrash(Jobs, job->JobId);
        YT_TLOG_INFO("RemoveJob during RemovePartition")
            .With("JobId", job->JobId)
            .With("PartitionId", oldPartition->PartitionId)
            .With("ComputationId", oldPartition->ComputationId);
        if (MutationNotifier_) {
            MutationNotifier_->OnRemoveJob(job, EJobFinishReason::PartitionUpdated);
        }
    }
    YT_TLOG_INFO("RemovePartition")
        .With("PartitionId", id);
    Partitions.erase(id);
    ++Updated_;
    if (MutationNotifier_) {
        MutationNotifier_->OnRemovePartition(oldPartition);
    }
}

void TFlowLayout::CreateJob(TJobPtr job)
{
    auto oldPartition = GetOrCrash(Partitions, job->PartitionId);
    Y_ENSURE(!oldPartition->CurrentJobId);
    auto newPartition = CloneYsonStruct(oldPartition);
    newPartition->CurrentJobId = job->JobId;
    YT_TLOG_INFO("CreateJob")
        .With("JobId", job->JobId)
        .With("PartitionId", job->PartitionId)
        .With("ComputationId", newPartition->ComputationId)
        .With("WorkerAddress", job->WorkerAddress);
    Partitions.insert_or_assign(newPartition->PartitionId, std::move(newPartition));
    THROW_ERROR_EXCEPTION_UNLESS(Jobs.emplace(job->JobId, job).second == true, "JobId duplicate");
    ++Updated_;
    if (MutationNotifier_) {
        MutationNotifier_->OnCreateJob(job);
    }
}

void TFlowLayout::UpdateJob(TJobPtr job)
{
    YT_TLOG_INFO("UpdateJob")
        .With("JobId", job->JobId)
        .With("PartitionId", job->PartitionId)
        .With("ComputationId", GetOrCrash(Partitions, job->PartitionId)->ComputationId)
        .With("WorkerAddress", job->WorkerAddress);
    // TODO: check GetOrCrash
    auto oldJob = GetOrCrash(Jobs, job->JobId);
    Jobs.insert_or_assign(job->JobId, job);
    ++Updated_;
    if (MutationNotifier_) {
        MutationNotifier_->OnUpdateJob(oldJob, job);
    }
}

void TFlowLayout::UpdateJob(TJobId id, TLeaseId leaseId)
{
    YT_TLOG_INFO("UpdateJobLease")
        .With("JobId", id)
        .With("LeaseId", leaseId);
    const auto& oldJob = GetOrCrash(Jobs, id);
    Y_ENSURE(oldJob->LeaseId == NullLeaseId);
    auto newJob = CloneYsonStruct(oldJob);
    newJob->LeaseId = leaseId;
    UpdateJob(newJob);
}

void TFlowLayout::RemoveJob(const TJobId& id, EJobFinishReason jobFinishReason)
{
    auto job = GetOrCrash(Jobs, id);
    auto oldPartition = GetOrCrash(Partitions, job->PartitionId);
    auto newPartition = CloneYsonStruct(oldPartition);
    newPartition->CurrentJobId = {};
    YT_TLOG_INFO("RemoveJob")
        .With("JobId", job->JobId)
        .With("PartitionId", job->PartitionId)
        .With("ComputationId", oldPartition->ComputationId)
        .With("WorkerAddress", job->WorkerAddress)
        .With("Reason", jobFinishReason);
    Partitions.insert_or_assign(newPartition->PartitionId, std::move(newPartition));
    EraseOrCrash(Jobs, id);
    ++Updated_;
    if (MutationNotifier_) {
        MutationNotifier_->OnRemoveJob(job, jobFinishReason);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TWorker::Register(TRegistrar registrar)
{
    registrar.Parameter("worker_groups", &TThis::Groups)
        .Default();
    registrar.Parameter("worker_capabilities", &TThis::Capabilities)
        .Default();
    registrar.Parameter("register_time", &TThis::RegisterTime)
        .Default();
    registrar.Parameter("address", &TThis::LegacyAddress)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TWatermarks::Register(TRegistrar registrar)
{
    registrar.Parameter("system_watermark", &TThis::SystemWatermark)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("event_watermark", &TThis::EventWatermark)
        .Default(ZeroSystemTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

void TWatermarkState::Register(TRegistrar registrar)
{
    registrar.Parameter("current_timestamp", &TThis::CurrentTimestamp)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("streams", &TThis::Streams)
        .Default();
    registrar.Parameter("alignment_groups", &TThis::AlignmentGroups)
        .Default();
}

TSystemTimestamp TWatermarkState::GetCurrentTimestamp() const
{
    return CurrentTimestamp;
}

TSystemTimestamp TWatermarkState::GetEventWatermark(const TStreamId& streamId) const
{
    auto iter = Streams.find(streamId);
    return iter != Streams.end() ? iter->second->EventWatermark : ZeroSystemTimestamp;
}

TSystemTimestamp TWatermarkState::GetSystemWatermark(const TStreamId& streamId) const
{
    auto iter = Streams.find(streamId);
    return iter != Streams.end() ? iter->second->SystemWatermark : ZeroSystemTimestamp;
}

TSystemTimestamp TWatermarkState::GetWatermark(const TStreamId& streamId, ETimeType timeType) const
{
    switch (timeType) {
        case ETimeType::EventTime:
            return GetEventWatermark(streamId);
        case ETimeType::SystemTime:
            return GetSystemWatermark(streamId);
        case ETimeType::CurrentTime:
            return CurrentTimestamp;
    }
}

TSystemTimestamp TWatermarkState::GetAlignmentEventWatermark(const TWatermarkAlignmentGroup& groupId) const
{
    auto iter = AlignmentGroups.find(groupId);
    return iter != AlignmentGroups.end() ? iter->second->EventWatermark : ZeroSystemTimestamp;
}

TSystemTimestamp TWatermarkState::GetAlignmentSystemWatermark(const TWatermarkAlignmentGroup& groupId) const
{
    auto iter = AlignmentGroups.find(groupId);
    return iter != AlignmentGroups.end() ? iter->second->SystemWatermark : ZeroSystemTimestamp;
}

////////////////////////////////////////////////////////////////////////////////

i64 TExecutionSpec::GetEpoch() const
{
    return PipelineState->GetVersion().Underlying() +
        PipelineSpec->GetVersion().Underlying() +
        ExtendedPipelineSpec->GetVersion().Underlying() +
        DynamicPipelineSpec->GetVersion().Underlying() +
        Layout->GetVersion().Underlying();
}

void TExecutionSpec::AttachToControl(TPersistedStateControlPtr<std::string> control)
{
    Layout->AttachToControl(std::move(control));
}

void TExecutionSpec::SetAsSlave()
{
    Layout->SetAsSlave();
}

TExecutionSpecPtr TExecutionSpec::Clone() const
{
    auto result = CloneYsonStruct(MakeStrong(this));
    result->Layout = Layout->Clone();
    return result;
}

void TExecutionSpec::SerializeAsYson(NYson::IYsonConsumer* consumer) const
{
    SerializeStructOverridingChild(*this, "layout", [this] (auto* c) {
        Layout->SerializeAsYson(c);
    },
        consumer);
}

void TExecutionSpec::StartMutation(TFlowStateMutationNotifierPtr notifier)
{
    Layout->StartMutation(std::move(notifier));
}

void TExecutionSpec::CreateSnapshot(bool committed)
{
    Layout->CreateSnapshot(committed);
}

void TExecutionSpec::CommitMutation(TPersistedStateCommitContext* context)
{
    Layout->CommitMutation(context);
}

void TExecutionSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_state", &TThis::PipelineState)
        .DefaultNew();

    registrar.Parameter("pipeline_spec", &TThis::PipelineSpec)
        .DefaultNew();

    registrar.Parameter("extended_pipeline_spec", &TThis::ExtendedPipelineSpec)
        .DefaultNew();

    registrar.Parameter("dynamic_pipeline_spec", &TThis::DynamicPipelineSpec)
        .DefaultNew();

    registrar.Parameter("layout", &TThis::Layout)
        .DefaultNew();

    registrar.Parameter("flow_core_target", &TThis::FlowCoreTarget)
        .DefaultNew();

    registrar.Parameter("stream_spec_storage_state", &TThis::StreamSpecStorageState)
        .DefaultNew();

    registrar.Parameter("input_streams_traverse", &TThis::InputStreamsTraverse)
        .DefaultNew();

    registrar.Parameter("watermark_state", &TThis::WatermarkState)
        .DefaultNew();
}

//! Checks whether the flow-core binary version matches the target set by user.
bool CheckFlowCoreTarget(const TFlowCoreTarget& flowCoreTarget, const std::string& actualFlowCoreVersion)
{
    if (flowCoreTarget.Underlying().empty()) {
        YT_TLOG_DEBUG("Skipping CheckFlowCoreTarget: flow core target is not set");
        return true;
    }

    if (flowCoreTarget.Underlying() == actualFlowCoreVersion) {
        YT_TLOG_DEBUG("Flow core version matches target")
            .With("FlowCoreVersion", actualFlowCoreVersion)
            .With("FlowCoreTarget", flowCoreTarget);

        return true;
    }

    YT_TLOG_INFO("Flow core version mismatches target")
        .With("FlowCoreVersion", actualFlowCoreVersion)
        .With("FlowCoreTarget", flowCoreTarget);

    return false;
}

bool CheckFlowCoreTarget(const TFlowViewPtr& flowView, const std::string& actualFlowCoreVersion)
{
    flowView->EnsureIsSynced();
    return CheckFlowCoreTarget(
        flowView->State->ExecutionSpec->FlowCoreTarget->GetValue(),
        actualFlowCoreVersion);
}

////////////////////////////////////////////////////////////////////////////////

i64 TExecutionSpecVersions::GetEpoch() const
{
    return PipelineStateVersion.Underlying() +
        PipelineSpecVersion.Underlying() +
        ExtendedPipelineSpecVersion.Underlying() +
        DynamicPipelineSpecVersion.Underlying() +
        LayoutVersion.Underlying();
}

void TExecutionSpecVersions::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_state_version", &TThis::PipelineStateVersion)
        .Default();

    registrar.Parameter("pipeline_spec_version", &TThis::PipelineSpecVersion)
        .Default();

    registrar.Parameter("extended_pipeline_spec_version", &TThis::ExtendedPipelineSpecVersion)
        .Default();

    registrar.Parameter("dynamic_pipeline_spec_version", &TThis::DynamicPipelineSpecVersion)
        .Default();

    registrar.Parameter("layout_version", &TThis::LayoutVersion)
        .Default();

    registrar.Parameter("stream_spec_storage_state_version", &TThis::StreamSpecStorageStateVersion)
        .Default();

    registrar.Parameter("input_traverse_data_version", &TThis::InputStreamsTraverseVersion)
        .Default();

    registrar.Parameter("watermark_state_version", &TThis::WatermarkStateVersion)
        .Default();
}

TExecutionSpecVersionsPtr BuildExecutionSpecVersions(const TExecutionSpecPtr& executionSpec)
{
    auto versions = New<TExecutionSpecVersions>();
    versions->PipelineStateVersion = executionSpec->PipelineState->GetVersion();
    versions->PipelineSpecVersion = executionSpec->PipelineSpec->GetVersion();
    versions->ExtendedPipelineSpecVersion = executionSpec->ExtendedPipelineSpec->GetVersion();
    versions->DynamicPipelineSpecVersion = executionSpec->DynamicPipelineSpec->GetVersion();
    versions->LayoutVersion = executionSpec->Layout->GetVersion();
    versions->StreamSpecStorageStateVersion = executionSpec->StreamSpecStorageState->GetVersion();
    versions->InputStreamsTraverseVersion = executionSpec->InputStreamsTraverse->GetVersion();
    versions->WatermarkStateVersion = executionSpec->WatermarkState->GetVersion();
    return versions;
}

std::pair<TExecutionSpecPtr, std::vector<TPersistedStateStorageRow<std::string>>> PrepareExecutionSpecUpdate(const TExecutionSpecPtr& current, const TExecutionSpecVersionsPtr& versions)
{
    auto update = New<TExecutionSpec>();

#define FLOW_VIEW_PREPARE_UPDATE(Field)                             \
    if (versions->Field##Version != current->Field->GetVersion()) { \
        update->Field = current->Field;                             \
    }

    FLOW_VIEW_PREPARE_UPDATE(PipelineState);
    FLOW_VIEW_PREPARE_UPDATE(PipelineSpec);
    FLOW_VIEW_PREPARE_UPDATE(ExtendedPipelineSpec);
    FLOW_VIEW_PREPARE_UPDATE(DynamicPipelineSpec);
    FLOW_VIEW_PREPARE_UPDATE(StreamSpecStorageState);
    FLOW_VIEW_PREPARE_UPDATE(InputStreamsTraverse);
    FLOW_VIEW_PREPARE_UPDATE(WatermarkState);

    std::vector<TPersistedStateStorageRow<std::string>> stateUpdate;
    if (versions->LayoutVersion != current->Layout->GetVersion()) {
        stateUpdate = current->Layout->Follow(TSequenceId{versions->LayoutVersion.Underlying()});
    }

#undef FLOW_VIEW_PREPARE_UPDATE

    return {std::move(update), std::move(stateUpdate)};
}

TExecutionSpecPtr ApplyExecutionSpecUpdate(const TExecutionSpecPtr& current, const TExecutionSpecPtr& update, const std::vector<TPersistedStateStorageRow<std::string>>& stateUpdate)
{
    auto newExecutionSpec = New<TExecutionSpec>();

#define FLOW_VIEW_APPLY_UPDATE(Field)                 \
    if (update->Field->GetVersion() != TVersion(0)) { \
        newExecutionSpec->Field = update->Field;      \
    } else {                                          \
        newExecutionSpec->Field = current->Field;     \
    }

    FLOW_VIEW_APPLY_UPDATE(PipelineState);
    FLOW_VIEW_APPLY_UPDATE(PipelineSpec);
    FLOW_VIEW_APPLY_UPDATE(ExtendedPipelineSpec);
    FLOW_VIEW_APPLY_UPDATE(DynamicPipelineSpec);
    FLOW_VIEW_APPLY_UPDATE(StreamSpecStorageState);
    FLOW_VIEW_APPLY_UPDATE(InputStreamsTraverse);
    FLOW_VIEW_APPLY_UPDATE(WatermarkState);

    newExecutionSpec->Layout = current->Layout;
    newExecutionSpec->Layout->Apply(stateUpdate);

#undef FLOW_VIEW_APPLY_UPDATE

    return newExecutionSpec;
}

////////////////////////////////////////////////////////////////////////////////

void TJobManagerState::Register(TRegistrar registrar)
{
    registrar.Parameter("computations", &TThis::Computations)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionJobStatus::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("last_traverse_data", &TThis::LastTraverseData)
        .Default();
    registrar.BaseClassParameter("last_partition_status", &TThis::LastPartitionStatus)
        .Default();
    registrar.BaseClassParameter("current_job_id", &TThis::CurrentJobId)
        .Default();
    registrar.BaseClassParameter("current_job_status_update_time", &TThis::CurrentJobStatusUpdateTime)
        .Default();
    registrar.BaseClassParameter("current_job_status", &TThis::CurrentJobStatus)
        .Default();
    registrar.BaseClassParameter("last_retryable_error_instant", &TThis::LastRetryableErrorInstant)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFlowStateMutationNotifier::OnCreatePartition(const TPartitionPtr& /*newPartition*/)
{ }

void TFlowStateMutationNotifier::OnUpdatePartition(const TPartitionPtr& /*oldPartition*/, const TPartitionPtr& /*newPartition*/)
{ }

void TFlowStateMutationNotifier::OnRemovePartition(const TPartitionPtr& /*oldPartition*/)
{ }

void TFlowStateMutationNotifier::OnCreateJob(const TJobPtr& /*newJob*/)
{ }

void TFlowStateMutationNotifier::OnUpdateJob(const TJobPtr& /*oldJob*/, const TJobPtr& /*newJob*/)
{ }

void TFlowStateMutationNotifier::OnRemoveJob(const TJobPtr& /*oldJob*/, EJobFinishReason /*reason*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TFlowFeedback::Register(TRegistrar registrar)
{
    registrar.Parameter("partition_job_statuses", &TThis::PartitionJobStatuses)
        .Default();
    registrar.Parameter("worker_statuses", &TThis::WorkerStatuses)
        .Default();
    registrar.Parameter("update_time", &TThis::UpdateTime)
        .Default();
}

const TJobStatusPtr& TFlowFeedback::GetCurrentJobStatus(const TPartitionId& partitionId) const
{
    auto it = PartitionJobStatuses.find(partitionId);
    if (it == PartitionJobStatuses.end()) {
        static TJobStatusPtr nothing;
        return nothing;
    }
    return it->second->CurrentJobStatus;
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicPartitionSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("finish_after_current_epoch", &TThis::FinishAfterCurrentEpoch)
        .Default(false);
    registrar.Parameter("computation_partition_spec", &TThis::ComputationPartitionSpec)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionEphemeralState::Register(TRegistrar registrar)
{
    registrar.Parameter("previous_job_finish_reason", &TThis::PreviousJobFinishReason)
        .Default(EJobFinishReason::Unknown);
    registrar.Parameter("last_ok_time", &TThis::LastOkTime)
        .Default();

    registrar.Parameter("previous_job_fail_instant", &TThis::PreviousJobFailInstant)
        .Default();
    registrar.Parameter("previous_job_fail_error", &TThis::PreviousJobFailError)
        .Default();

    registrar.Parameter("previous_rebalancing_instant", &TThis::PreviousRebalancingInstant)
        .Default();

    registrar.Parameter("dynamic_partition_spec", &TThis::DynamicPartitionSpec)
        .DefaultNew();

    registrar.Parameter("pending_graceful_rebalance_worker_address", &TThis::PendingGracefulRebalanceWorkerAddress)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TStreamTraverseDataMetrics::Register(TRegistrar registrar)
{
    registrar.Parameter("system_watermark_min_max_difference", &TThis::SystemWatermarkMinMaxDifference)
        .Default(0.0);
    registrar.Parameter("event_watermark_min_max_difference", &TThis::EventWatermarkMinMaxDifference)
        .Default(0.0);
}

////////////////////////////////////////////////////////////////////////////////

void TStreamSpeedStatistics::Register(TRegistrar registrar)
{
    registrar.Parameter("processed_messages_per_second", &TThis::ProcessedMessagesPerSecond)
        .Default(0.0);
    registrar.Parameter("processed_bytes_per_second", &TThis::ProcessedBytesPerSecond)
        .Default(0.0);
}

////////////////////////////////////////////////////////////////////////////////

void TPipelineSpeedStatistics::Register(TRegistrar registrar)
{
    registrar.Parameter("last_updated", &TThis::LastUpdated)
        .Default(ZeroSystemTimestamp);
    registrar.Parameter("stream_speed_1d", &TThis::StreamSpeed1d)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMessageTransferingInfo::Register(TRegistrar registrar)
{
    registrar.Parameter("stream_timestamp_statistics", &TThis::StreamTimestampStatistics)
        .Default();
    registrar.Parameter("speed_statistics", &TThis::SpeedStatistics)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

const TPartitionEphemeralStatePtr& TFlowEphemeralState::GetPartitionState(const TPartitionId& partitionId)
{
    auto& state = Partitions[partitionId];
    if (!state) {
        state = New<TPartitionEphemeralState>();
    }
    return state;
}

void TFlowEphemeralState::Register(TRegistrar registrar)
{
    registrar.Parameter("partitions", &TThis::Partitions)
        .Default();
    registrar.Parameter("stream_traverse_data_metrics", &TThis::StreamTraverseDataMetrics)
        .Default();
    registrar.Parameter("max_applied_balancer_sequence_ids", &TThis::MaxAppliedBalancerSequenceIds)
        .Default();
    registrar.Parameter("worker_incarnations_jobs", &TThis::WorkerIncarnationsJobs)
        .Default();
    registrar.Parameter("message_transfering_info", &TThis::MessageTransferingInfo)
        .DefaultNew();
    registrar.Parameter("pipeline_path", &TThis::PipelinePath)
        .Default();
    registrar.Parameter("traverse_uncovered_computations", &TThis::TraverseUncoveredComputations)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFlowState::AttachToControl(TPersistedStateControlPtr<std::string> control)
{
    ExecutionSpec->AttachToControl(std::move(control));
}

void TFlowState::SetAsSlave()
{
    ExecutionSpec->SetAsSlave();
}

TFlowStatePtr TFlowState::Clone() const
{
    auto result = CloneYsonStruct(MakeStrong(this));
    result->ExecutionSpec = ExecutionSpec->Clone();
    return result;
}

void TFlowState::SerializeAsYson(NYson::IYsonConsumer* consumer) const
{
    SerializeStructOverridingChild(*this, "execution_spec", [this] (auto* c) {
        ExecutionSpec->SerializeAsYson(c);
    },
        consumer);
}

void TFlowState::StartMutation(TFlowStateMutationNotifierPtr notifier)
{
    ExecutionSpec->StartMutation(std::move(notifier));
}

void TFlowState::CreateSnapshot(bool committed)
{
    ExecutionSpec->CreateSnapshot(committed);
}

void TFlowState::CommitMutation(TPersistedStateCommitContext* context)
{
    ExecutionSpec->CommitMutation(context);
}

////////////////////////////////////////////////////////////////////////////////

void TFlowState::Register(TRegistrar registrar)
{
    registrar.Parameter("current_timestamp", &TThis::CurrentTimestamp)
        .Default();

    registrar.Parameter("execution_spec", &TThis::ExecutionSpec)
        .DefaultNew();

    registrar.Parameter("epoch", &TThis::Epoch)
        .Default();

    registrar.Parameter("workers", &TThis::Workers)
        .Default();

    registrar.Parameter("traverse_data", &TThis::TraverseData)
        .DefaultNew();

    registrar.Parameter("job_manager_state", &TThis::JobManagerState)
        .DefaultNew();

    registrar.Parameter("speed_statistics", &TThis::SpeedStatistics)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TFlowView::SerializeAsYson(NYson::IYsonConsumer* consumer) const
{
    SerializeStructOverridingChild(*this, "state", [this] (auto* c) {
        State->SerializeAsYson(c);
    },
        consumer);
}

TYsonString TFlowView::SerializeAsYsonString() const
{
    TString result;
    TStringOutput out(result);
    NYson::TBufferedBinaryYsonWriter writer(&out);
    SerializeAsYson(&writer);
    writer.Flush();
    return TYsonString(result);
}

TFlowViewPtr TFlowView::LoadFromNode(const NYTree::INodePtr& node, TPersistedStateControlPtr<std::string> control)
{
    auto flowView = ConvertTo<TFlowViewPtr>(node);

    // SerializeAsYson renders the layout as plain "partitions"/"jobs" maps that TFlowLayout does not
    // deserialize back, so they are replayed here from the raw node.
    auto layoutNode = FindNodeByYPath(node, "/state/execution_spec/layout");
    if (!layoutNode) {
        return flowView;
    }

    auto layoutMap = layoutNode->AsMap();
    auto partitionsNode = layoutMap->FindChild("partitions");
    auto jobsNode = layoutMap->FindChild("jobs");
    if (!partitionsNode && !jobsNode) {
        return flowView;
    }

    // The caller supplies a control with throwaway (in-memory) storage; Recover() reads nothing, so the
    // layout starts empty and is populated solely from the node below.
    flowView->State->AttachToControl(control);
    control->Recover();

    // Replay through the public mutation API in one committed transaction: create the partitions
    // first, then the jobs, which re-link their partitions via job->PartitionId.
    flowView->State->StartMutation();
    const auto& layout = flowView->State->ExecutionSpec->Layout;
    if (partitionsNode) {
        for (auto& [partitionId, partition] : ConvertTo<THashMap<TPartitionId, TPartitionPtr>>(partitionsNode)) {
            // CreatePartition() forbids a pre-linked job; CreateJob() below restores the link.
            partition->CurrentJobId.reset();
            layout->CreatePartition(std::move(partition));
        }
    }
    if (jobsNode) {
        for (auto& [jobId, job] : ConvertTo<THashMap<TJobId, TJobPtr>>(jobsNode)) {
            layout->CreateJob(std::move(job));
        }
    }
    flowView->State->CommitMutation();

    return flowView;
}

TFlowViewPtr TFlowView::CopyPtr() const
{
    auto copy = New<TFlowView>();
    static_cast<TFlowViewBase&>(*copy) = *this;
    return copy;
}

void TFlowView::Register(TRegistrar registrar)
{
    registrar.BaseClassParameter("feedback", &TFlowView::Feedback)
        .DefaultNew();

    registrar.BaseClassParameter("ephemeral_state", &TFlowView::EphemeralState)
        .DefaultNew();

    registrar.BaseClassParameter("state", &TFlowView::State)
        .DefaultNew();

    registrar.BaseClassParameter("current_spec", &TFlowView::CurrentSpec)
        .DefaultNew();

    registrar.BaseClassParameter("current_dynamic_spec", &TFlowView::CurrentDynamicSpec)
        .DefaultNew();

    // ConvertTo does not rebuild the layout's Partitions/Jobs; callers that need a queryable layout
    // use the static LoadFromNode() factory, which deserializes and rebuilds the layout in one call.
}

////////////////////////////////////////////////////////////////////////////////

static TSharedRef CompressFlowViewYson(const TYsonString& ysonString, NCompression::ECodec codec)
{
    return NCompression::GetCodec(codec)->Compress(TSharedRef::FromString(ysonString.ToString()));
}

static NCompression::ECodec GetFlowViewCacheCodec(const TFlowViewPtr& flowView)
{
    return flowView->CurrentDynamicSpec->GetValue()->FlowViewCacheCodec;
}

TFlowViewKeeper::TFlowViewKeeper()
    : FlowView_(New<TFlowView>())
{ }

TFlowViewKeeper::~TFlowViewKeeper() = default;

void TFlowViewKeeper::Init(TFlowStatePtr state, TFlowEphemeralStatePtr ephemeralState, TVersionedPipelineSpecPtr spec, TVersionedDynamicPipelineSpecPtr dynamicSpec)
{
    YT_VERIFY(state);
    YT_VERIFY(spec);
    YT_VERIFY(dynamicSpec);
    auto guard = Guard(Lock_);
    FlowView_ = New<TFlowView>();
    FlowView_->State = std::move(state);
    FlowView_->State->CreateSnapshot();
    FlowView_->EphemeralState = std::move(ephemeralState);
    FlowView_->CurrentSpec = std::move(spec);
    FlowView_->CurrentDynamicSpec = std::move(dynamicSpec);
    CachedYsonString_ = FlowView_->SerializeAsYsonString();
    CachedYsonIndex_ = TIndexedYsonString::Build(CachedYsonString_, FlowViewHybridLeafSizeThreshold);
    CachedCompressionCodec_ = GetFlowViewCacheCodec(FlowView_);
    CachedCompressedFlowView_ = CompressFlowViewYson(CachedYsonString_, CachedCompressionCodec_);
}

void TFlowViewKeeper::Reset()
{
    auto guard = Guard(Lock_);
    FlowView_ = nullptr;
    CachedYsonString_ = TYsonString();
    CachedYsonIndex_ = nullptr;
    CachedCompressedFlowView_ = {};
}

bool TFlowViewKeeper::SetFeedback(TFlowFeedbackPtr feedback, TVersion expectedSpecVersion)
{
    YT_VERIFY(feedback);
    auto guard = Guard(Lock_);
    EnsureInit(guard);
    if (FlowView_->CurrentSpec->GetVersion() != expectedSpecVersion) {
        return false;
    }
    auto copy = FlowView_->CopyPtr();
    copy->Feedback = std::move(feedback);
    std::swap(copy, FlowView_);
    return true;
}

void TFlowViewKeeper::SetStates(TFlowStatePtr state, TFlowEphemeralStatePtr ephemeralState)
{
    YT_VERIFY(state);
    auto guard = Guard(Lock_);
    EnsureInit(guard);
    auto copy = FlowView_->CopyPtr();
    copy->State = std::move(state);
    copy->State->CreateSnapshot();
    copy->EphemeralState = std::move(ephemeralState);
    std::swap(copy, FlowView_);
}

void TFlowViewKeeper::SetFlowCoreTarget(TVersionedFlowCoreTargetPtr flowCoreTarget)
{
    YT_VERIFY(flowCoreTarget);
    auto guard = Guard(Lock_);
    EnsureInit(guard);
    auto copy = FlowView_->CopyPtr();
    copy->State = copy->State->Clone();
    copy->State->ExecutionSpec->FlowCoreTarget = std::move(flowCoreTarget);
    copy->State->CreateSnapshot();
    std::swap(copy, FlowView_);
}

void TFlowViewKeeper::SetSpecs(
    std::optional<TVersionedPipelineSpecPtr> spec,
    std::optional<TVersionedDynamicPipelineSpecPtr> dynamicSpec)
{
    auto guard = Guard(Lock_);
    EnsureInit(guard);
    auto copy = FlowView_->CopyPtr();
    bool pipelineSpecVersionChanged = false;
    if (spec) {
        YT_VERIFY(*spec);
        if (copy->CurrentSpec->GetVersion() != (*spec)->GetVersion()) {
            pipelineSpecVersionChanged = true;
        }
        copy->CurrentSpec = std::move(*spec);
    }
    if (dynamicSpec) {
        YT_VERIFY(*dynamicSpec);
        copy->CurrentDynamicSpec = std::move(*dynamicSpec);
    }
    if (pipelineSpecVersionChanged) {
        // Static spec change is only accepted on a paused/stopped pipeline, so worker-side jobs of the
        // previous version are gone (worker drops them in worker/job_tracker.cpp on its own version
        // bump). Any feedback persisted under the previous spec is therefore obsolete and unsafe to
        // validate against the new spec - drop it.
        copy->Feedback = New<TFlowFeedback>();
    }
    std::swap(copy, FlowView_);
}

TFlowViewPtr TFlowViewKeeper::GetFlowView() const
{
    auto guard = Guard(Lock_);
    EnsureInit(guard);
    return FlowView_;
}

TYsonString TFlowViewKeeper::GetYsonStringByPath(const NYPath::TYPath& path, bool cache) const
{
    if (cache) {
        TIndexedYsonStringPtr index;
        {
            auto guard = Guard(Lock_);
            EnsureInit(guard);
            index = CachedYsonIndex_;
        }
        return index->GetByPath(path);
    }
    // Fresh (uncached) view: extract the sub-path by streaming, without an index.
    auto result = NYTree::TryGetAny(GetYsonString(cache).AsStringBuf(), path);
    if (!result) {
        THROW_ERROR_EXCEPTION("Flow view has no node at %v", path);
    }
    return TYsonString(std::move(*result));
}

TYsonString TFlowViewKeeper::GetYsonString(bool cache) const
{
    if (cache) {
        auto guard = Guard(Lock_);
        EnsureInit(guard);
        return CachedYsonString_;
    }
    return GetFlowView()->SerializeAsYsonString();
}

TCompressedFlowView TFlowViewKeeper::GetCompressedYsonString() const
{
    auto guard = Guard(Lock_);
    EnsureInit(guard);
    return TCompressedFlowView{
        .Data = CachedCompressedFlowView_,
        .Codec = CachedCompressionCodec_,
    };
}

TCompressedFlowView TFlowViewKeeper::CompressYson(const TYsonString& yson) const
{
    auto codec = GetFlowViewCacheCodec(GetFlowView());
    return TCompressedFlowView{
        .Data = CompressFlowViewYson(yson, codec),
        .Codec = codec,
    };
}

void TFlowViewKeeper::RebuildNodeCache(const IInvokerPtr& invoker)
{
    auto flowView = GetFlowView();
    auto ysonString = flowView->SerializeAsYsonString();
    auto codec = GetFlowViewCacheCodec(flowView);
    // Build the by-path index in parallel with the compression -- both are O(P*S) passes over the string.
    auto indexFuture = BIND(&TIndexedYsonString::Build, ysonString, FlowViewHybridLeafSizeThreshold)
        .AsyncVia(invoker)
        .Run();
    auto compressed = CompressFlowViewYson(ysonString, codec);
    auto ysonIndex = WaitForFast(indexFuture).ValueOrThrow();
    {
        auto guard = Guard(Lock_);
        if (FlowView_) {
            std::swap(CachedYsonString_, ysonString);
            std::swap(CachedYsonIndex_, ysonIndex);
            std::swap(CachedCompressedFlowView_, compressed);
            CachedCompressionCodec_ = codec;
        }
    }
}

void TFlowViewKeeper::EnsureInit(TGuard<NThreading::TSpinLock>& /*guard*/) const
{
    if (!FlowView_ || !CachedYsonString_ || !CachedYsonIndex_ || !CachedCompressedFlowView_) {
        THROW_ERROR_EXCEPTION(EErrorCode::FlowViewKeeperIsNotInitialized, "FlowViewKeeper is not initialized");
    }
}

////////////////////////////////////////////////////////////////////////////////

void TPipelineImportantVersions::Register(TRegistrar registrar)
{
    registrar.Parameter("pipeline_spec_versions", &TThis::PipelineSpecVersion)
        .Default();
    registrar.Parameter("pipeline_state_versions", &TThis::PipelineStateVersion)
        .Default();
    registrar.Parameter("flow_core_target_version", &TThis::FlowCoreTargetVersion)
        .Default();
}

void TPipelineImportantVersions::EnsureEqual(const TPipelineImportantVersions& actual) const
{
    THROW_ERROR_EXCEPTION_UNLESS(
        PipelineSpecVersion == actual.PipelineSpecVersion,
        NFlow::EErrorCode::SpecVersionMismatch,
        "Spec version mismatch (Expected: %v, Actual: %v)",
        PipelineSpecVersion,
        actual.PipelineSpecVersion);
    THROW_ERROR_EXCEPTION_UNLESS(
        PipelineStateVersion == actual.PipelineStateVersion,
        NFlow::EErrorCode::PipelineStateVersionMismatch,
        "Pipeline state version mismatch (Expected: %v, Actual: %v)",
        PipelineStateVersion,
        actual.PipelineStateVersion);
    THROW_ERROR_EXCEPTION_UNLESS(
        FlowCoreTargetVersion == actual.FlowCoreTargetVersion,
        NFlow::EErrorCode::FlowCoreTargetVersionMismatch,
        "FlowCoreTarget version mismatch (Expected: %v, Actual: %v)",
        FlowCoreTargetVersion,
        actual.FlowCoreTargetVersion);

    YT_VERIFY(*this == actual);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
