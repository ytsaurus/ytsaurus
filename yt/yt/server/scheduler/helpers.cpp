#include "helpers.h"

#include "public.h"
#include "exec_node.h"
#include "allocation.h"
#include "operation.h"

#include <yt/yt/server/lib/scheduler/config.h>
#include <yt/yt/server/lib/scheduler/experiments.h>
#include <yt/yt/server/lib/scheduler/helpers.h>

#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/serialize.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;
using namespace NYPath;
using namespace NYson;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NConcurrency;
using namespace NSecurityClient;
using namespace NChunkClient;
using namespace NApi;
using namespace NLogging;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

void BuildMinimalOperationAttributes(TOperationPtr operation, TFluentMap fluent)
{
    fluent
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpecString())
        .Item("provided_spec").Value(operation->ProvidedSpecString())
        .Item("experiment_assignments").Value(operation->ExperimentAssignments())
        .Item("experiment_assignment_names").Value(operation->GetExperimentAssignmentNames())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Item("user_transaction_id").Value(operation->GetUserTransactionId())
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended());
}

void BuildFullOperationAttributes(TOperationPtr operation, bool includeOperationId, bool includeHeavyAttributes, TFluentMap fluent)
{
    const auto& initializationAttributes = operation->ControllerAttributes().InitializeAttributes;
    const auto& prepareAttributes = operation->ControllerAttributes().PrepareAttributes;
    fluent
        .DoIf(includeOperationId, [&] (TFluentMap fluent) {
            fluent.Item("operation_id").Value(operation->GetId());
        })
        .Item("operation_type").Value(operation->GetType())
        .Item("start_time").Value(operation->GetStartTime())
        .Item("spec").Value(operation->GetSpecString())
        .Item("provided_spec").Value(operation->ProvidedSpecString())
        .Item("experiment_assignments").Value(operation->ExperimentAssignments())
        .Item("experiment_assignment_names").Value(operation->GetExperimentAssignmentNames())
        .Item("authenticated_user").Value(operation->GetAuthenticatedUser())
        .Item("mutation_id").Value(operation->GetMutationId())
        .Item("user_transaction_id").Value(operation->GetUserTransactionId())
        .DoIf(initializationAttributes && includeHeavyAttributes, [&] (TFluentMap fluent) {
            fluent
                .Item("unrecognized_spec").Value(initializationAttributes->UnrecognizedSpec)
                .Item("full_spec").Value(initializationAttributes->FullSpec);
        })
        .DoIf(static_cast<bool>(prepareAttributes), [&] (TFluentMap fluent) {
            fluent
                .Items(prepareAttributes);
        })
        .Item("task_names").Value(operation->GetTaskNames())
        .Do(BIND(&BuildMutableOperationAttributes, operation));
}

void BuildMutableOperationAttributes(TOperationPtr operation, TFluentMap fluent)
{
    auto initializationAttributes = operation->ControllerAttributes().InitializeAttributes;
    fluent
        .Item("state").Value(operation->GetState())
        .Item("suspended").Value(operation->GetSuspended())
        .Item("events").Value(operation->Events())
        .Item("slot_index_per_pool_tree").Value(operation->GetSlotIndices())
        .DoIf(static_cast<bool>(initializationAttributes), [&] (TFluentMap fluent) {
            fluent
                .Items(initializationAttributes->Mutable);
        });
}

////////////////////////////////////////////////////////////////////////////////

TString MakeOperationCodicilString(TOperationId operationId)
{
    return Format("OperationId: %v", operationId);
}

TCodicilGuard MakeOperationCodicilGuard(TOperationId operationId)
{
    return TCodicilGuard(MakeOperationCodicilString(operationId));
}

////////////////////////////////////////////////////////////////////////////////

TListOperationsResult ListOperations(
    TCallback<TObjectServiceProxy::TReqExecuteBatchPtr()> createBatchRequest)
{
    using NYT::ToProto;

    static const std::vector<TString> attributeKeys = {
        "state"
    };

    auto batchReq = createBatchRequest();

    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto hashStr = Format("%02x", hash);
        auto req = TYPathProxy::List("//sys/operations/" + hashStr);
        ToProto(req->mutable_attributes()->mutable_keys(), attributeKeys);
        batchReq->AddRequest(req, "list_operations_" + hashStr);
    }

    auto batchRsp = WaitFor(batchReq->Invoke())
        .ValueOrThrow();

    TListOperationsResult result;

    THashSet<TOperationId> operationIds;
    for (int hash = 0x0; hash <= 0xFF; ++hash) {
        auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspList>(
            "list_operations_" + Format("%02x", hash));

        if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            continue;
        }

        auto hashBucketRsp = rspOrError.ValueOrThrow();

        try {
            auto hashBucketListNode = ConvertToNode(TYsonString(hashBucketRsp->value()));
            auto hashBucketList = hashBucketListNode->AsList();

            for (const auto& operationNode : hashBucketList->GetChildren()) {
                auto id = operationNode->GetValue<TOperationId>();
                YT_VERIFY((id.Underlying().Parts32[0] & 0xff) == hash);

                auto state = operationNode->Attributes().Get<EOperationState>("state");
                EmplaceOrCrash(operationIds, id);

                if (IsOperationInProgress(state)) {
                    result.OperationsToRevive.push_back({id, state});
                } else {
                    result.OperationsToArchive.push_back(id);
                }
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing operations from //sys/operations/%02x", hash)
                << ex;
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TJobResources ComputeAvailableResources(
    const TJobResources& resourceLimits,
    const TJobResources& resourceUsage,
    const TJobResources& resourceDiscount)
{
    return resourceLimits - resourceUsage + resourceDiscount;
}

////////////////////////////////////////////////////////////////////////////////

TOperationFairShareTreeRuntimeParametersPtr GetSchedulingOptionsPerPoolTree(IOperationStrategyHost* operation, const TString& treeId)
{
    return GetOrCrash(operation->GetRuntimeParameters()->SchedulingOptionsPerPoolTree, treeId);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TStatisticsDescription
{
    TString Name;
    TString Description;
    TString Unit;
};

const std::vector<TStatisticsDescription>& GetOperationStatisticsDescriptions()
{
    static const std::vector<TStatisticsDescription> statisticsHints = {
        {"time/total", "Time from the moment of job creation until the scheduler receives information about its completion or failure", "ms"},
        {"time/prepare", "Time of job preparation before the job proxy is launched", "ms"},
        {"time/artifacts_download", "Job's artifact files downloading to the chunk cache duration", "ms"},
        {"time/prepare_root_fs", "Root Porto volume preparation duration", "ms"},
        {"time/gpu_check", "GPU liveness check duration", "ms"},
        {"time/exec", "Time from the start to the end of job_proxy process", "ms"},

        {"data/input/chunk_count", "Data slices read by job", "pieces"},
        {"data/input/row_count", "Number of rows read by job", "pieces"},
        {"data/input/compressed_data_size", "Compressed size of blocks read by job", "bytes"},
        {"data/input/uncompressed_data_size", "Uncompressed size of blocks read by job", "bytes"},
        {"data/input/data_weight", "Logical size of data read by job", "bytes"},
        {"data/input/not_fully_consumed", "1 if job has not read the whole input, 0 otherwise", ""},
        {"data/input/regular_disk_space", "Not used", ""},
        {"data/input/erasure_disk_space", "Not used", ""},
        {"data/input/unmerged_data_weight", "Only for dynamic tables, logical size of unmerged data read by job", "bytes"},
        {"data/input/unmerged_row_count", "Only for dynamic tables, number of unmerged rows read by job", "pieces"},
        {"data/input/encoded_row_batch_count", "Only for arrow input format, number of encoded row batches", "pieces"},
        {"data/input/encoded_columnar_batch_count", "Only for arrow input format, number of encoded columnar batches", "pieces"},

        {"data/output/*/chunk_count", "Number of chunks written to the output table", "pieces"},
        {"data/output/*/row_count", "Number of rows written to the output table", "pieces"},
        {"data/output/*/compressed_data_size", "Compressed size of blocks written to the output table", "bytes"},
        {"data/output/*/uncompressed_data_size", "Uncompressed size of blocks written to the output table", "bytes"},
        {"data/output/*/data_weight", "Logical size of data written to the output table", "bytes"},
        {"data/output/*/regular_disk_space", "Only for tables with erasure_codec == none, equals to compressed_data_size + size of the chunk metadata", "bytes"},
        {"data/output/*/erasure_disk_space", "Only for tables with erasure_codec == none, equals to compressed_data_size + size of parity blocks", "bytes"},
        {"data/output/*/unmerged_data_weight", "Only for dynamic tables, logical size of unmerged data written to the output table", "bytes"},
        {"data/output/*/unmerged_row_count", "Only for dynamic tables, number of unmerged rows written to the output table", "pieces"},

        {"exec_agent/traffic/*_to_*", "Data volume transferred between these datacenters", "bytes"},
        {"exec_agent/traffic/duration_ms", "Time during which data was being transferred", "ms"},
        {"exec_agent/traffic/inbound/from_*", "Volume of exec agent's inbound traffic from this data center, typically, job artifacts", "bytes"},
        {"exec_agent/traffic/outbound/to_*", "Volume of exec agent's outbound traffic to this data center", "bytes"},

        {"exec_agent/artifacts/cache_bypassed_artifacts_size", "Size of artifacts with bypass_artifact_cache == true", "bytes"},
        {"exec_agent/artifacts/cache_hit_artifacts_size", "Size of artifacts retrieved from cache", "bytes"},
        {"exec_agent/artifacts/cache_miss_artifacts_size", "Size of artifacts that were not found in cache", "bytes"},

        {"job_proxy/cumulative_estimated_memory", "Memory for the job proxy estimated by scheduler/controller agent multiplied by job length in seconds", "bytes * sec"},
        {"job_proxy/cumulative_memory_mb_sec", "Integral of the memory used", "MB*sec"},
        {"job_proxy/cumulative_max_memory", "Maximum amount of memory used by the job proxy process multiplied by job length in seconds", "bytes * sec"},
        {"job_proxy/cumulative_memory_reserve", "Amount of memory reserved for job proxy at the time of start multiplied by job length in seconds", "bytes * sec"},
        {"job_proxy/estimated_memory", "Memory for the job proxy estimated by scheduler/controller agent", "bytes"},
        // TODO(renadeen): Maybe elaborate on CPU monitor's statistics?
        {"job_proxy/aggregated_max_cpu_usage_x100", "Internal statistic of job CPU monitor", ""},
        {"job_proxy/aggregated_smoothed_cpu_usage_x100", "Internal statistic of job CPU monitor", ""},
        {"job_proxy/aggregated_preemptible_cpu_x100", "Internal statistic of job CPU monitor", ""},
        {"job_proxy/aggregated_preempted_cpu_x100", "Internal statistic of job CPU monitor", ""},
        {"job_proxy/preemptible_cpu_x100", "Internal statistic of job CPU monitor", ""},
        // COMPAT(eshcherbin)
        {"job_proxy/aggregated_preemptable_cpu_x100", "Internal statistic of job CPU monitor", ""},
        {"job_proxy/preemptable_cpu_x100", "Internal statistic of job CPU monitor", ""},
        {"job_proxy/smoothed_cpu_usage_x100", "Internal statistic of job CPU monitor", ""},
        {"job_proxy/memory_reserve_factor_x10000", "Internal statistic", ""},
        {"job_proxy/max_memory", "Maximum amount of memory used by the job proxy process", "bytes"},
        {"job_proxy/memory_reserve", "Amount of memory guaranteed for the job proxy at the time of start", "bytes"},

        {"job_proxy/cpu/user", "User mode CPU time of the job proxy process", "ms"},
        {"job_proxy/cpu/system", "Kernel mode CPU time of the job proxy process", "ms"},
        {"job_proxy/cpu/wait", "Wait CPU time of the job proxy process", "ms"},
        {"job_proxy/cpu/throttled", "Throttled CPU time of the job proxy process", "ms"},
        {"job_proxy/cpu/peak_thread_count", "Maximum number of threads used by the job proxy process", "pieces"},
        {"job_proxy/cpu/context_switches", "Number of context switches performed by the job proxy process", "pieces"},

        {"job_proxy/traffic/*_to_*", "Data volume transferred between these datacenters", "bytes"},
        {"job_proxy/traffic/duration_ms", "Time during which data was being transferred", "ms"},
        {"job_proxy/traffic/inbound/from_*", "Volume of job proxy's inbound traffic from this data center, typically, job's input", "bytes"},
        {"job_proxy/traffic/outbound/to_*", "Volume of job proxy's outbound traffic to this data center, typically, job's output", "bytes"},

        {"job_proxy/block_io/bytes_written", "Bytes written by the job proxy process to the local block device", "bytes"},
        {"job_proxy/block_io/bytes_read", "Bytes read by the job proxy process from the local block device", "bytes"},
        {"job_proxy/block_io/io_read", "Number of reads from the local block device by the job proxy process", "pieces"},
        {"job_proxy/block_io/io_write", "Number of writes to the local block device by the job proxy process", "pieces"},
        {"job_proxy/block_io/io_total", "Number of input/output operations with the local block device by the job proxy process", "pieces"},

        {"user_job/cumulative_max_memory", "Maximum amount of memory used by the job multiplied by job length in seconds", "bytes * sec"},
        {"user_job/cumulative_memory_reserve", "Amount of memory guaranteed to the user job at the time of start multiplied by job length in seconds", "bytes * sec"},
        {"user_job/cumulative_memory_mb_sec", "Integral of the memory used", "MB*sec"},
        {"user_job/tmpfs_size", "Current (or final) amount of tmpfs used by user job", "bytes"},
        {"user_job/max_tmpfs_size", "Maximum amount of tmpfs used by user job during execution", "bytes"},
        {"user_job/max_memory", "Maximum amount of memory used by the user job during execution, excluding tmpfs", "bytes"},
        {"user_job/memory_limit", "Memory limit from operation specification", "bytes"},
        {"user_job/memory_reserve", "Amount of memory guaranteed to the user job at the time of start", "bytes"},
        {"user_job/memory_reserve_factor_x10000", "Internal statistics", ""},
        {"user_job/woodpecker", "Not used", ""},

        {"user_job/tmpfs_volumes/*/size", "Current (or final) amount of tmpfs used by user job in this volume", "bytes"},
        {"user_job/tmpfs_volumes/*/max_size", "Maximum amount of tmpfs used by user job in this volume during execution", "bytes"},

        {"user_job/cpu/user", "User mode CPU time of the job", "ms"},
        {"user_job/cpu/system", "Kernel mode CPU time of the job", "ms"},
        {"user_job/cpu/wait", "Wait CPU time of the job", "ms"},
        {"user_job/cpu/throttled", "Throttled CPU time of the job", "ms"},
        {"user_job/cpu/peak_thread_count", "Maximum number of threads used by the job", "pieces"},
        {"user_job/cpu/context_switches", "Number of context switches performed by the job", "pieces"},

        {"user_job/block_io/bytes_written", "Bytes written by the job to the local block device", "bytes"},
        {"user_job/block_io/bytes_read", "Bytes read by the job from the local block device", "bytes"},
        {"user_job/block_io/io_read", "Number of reads from the local block device by the job", "pieces"},
        {"user_job/block_io/io_write", "Number of writes to the local block device by the job", "pieces"},
        {"user_job/block_io/io_total", "Number of input/output operations with the local block device by the job proxy process", "pieces"},

        {"user_job/current_memory/major_page_faults", "Major page faults in the user process", "pieces"},
        {"user_job/current_memory/rss", "RSS at the end of the job", "bytes"},
        {"user_job/current_memory/mapped_file", "Memory mapped files size at the end of the job", "bytes"},

        {"user_job/disk/max_usage", "Maximum disk space used by the job sandbox", "bytes"},
        {"user_job/disk/limit", "Ordered limit on the size of the job sandbox", "bytes"},

        {"user_job/pipes/input/bytes", "Number of bytes transmitted via the stdin of the user process", "bytes"},
        {"user_job/pipes/input/idle_time", "Time during which the job proxy did not transfer data to the user's job in stdin because it was reading data", "ms"},
        {"user_job/pipes/input/busy_time", "Time during which the job proxy wrote data to the stdin of the user job", "ms"},

        {"user_job/pipes/output/*/bytes", "Number of bytes written by the user process to the corresponding descriptor", "bytes"},
        {"user_job/pipes/output/*/idle_time", "Time during which the job proxy process did not read from the stream corresponding to the k-th output table, because it was writing data already subtracted from there", "ms"},
        {"user_job/pipes/output/*/busy_time", "Time during which the job proxy process read from the stream corresponding to the k-th output table", "ms"},

        // COMPAT(ignat)
        {"user_job/gpu/utilization_gpu", "Net time during which execution on GPU were performed", "ms"},
        {"user_job/gpu/utilization_memory", "Net time during which GPU memory accesses were performed", "ms"},
        {"user_job/gpu/utilization_power", "Time integral of the effective power of GPU relative to the maximum power", "ratio * ms"},
        {"user_job/gpu/sm_utilization", "Time integral of SM processors utiliaztion share", "ratio * ms"},
        {"user_job/gpu/sm_occupancy", "Time integral of SM processors occupancy share", "ratio * ms"},
        {"user_job/gpu/load", "Time during which GPU load was non-zero", "ms"},
        {"user_job/gpu/memory", "Integral of GPU memory usage", "ms * bytes"},
        {"user_job/gpu/power", "Integral of GPU power usage", "ms * power"},
        {"user_job/gpu/clocks_sm", "Integral of GPU frequency usage", "ms * frequency"},
        {"user_job/gpu/memory_used", "Maximum registered GPU memory usage", "bytes"},

        {"user_job/gpu/cumulative_utilization_gpu", "Net time during which GPU calculations were performed", "ms"},
        {"user_job/gpu/cumulative_utilization_memory", "Net time during which GPU memory accesses were performed", "ms"},
        {"user_job/gpu/cumulative_utilization_clocks_sm", "The time integral of GPU frequency relative to the maximum frequency", "ratio * ms"},
        {"user_job/gpu/cumulative_utilization_power", "Time integral of the effective power of GPU relative to the maximum power", "ratio * ms"},
        {"user_job/gpu/cumulative_load", "Time during which GPU load was non-zero", "ms"},
        {"user_job/gpu/cumulative_memory", "Integral of GPU memory usage", "ms * bytes"},
        {"user_job/gpu/cumulative_memory_mb_sec", "Integral of GPU memory usage", "sec * MB"},
        {"user_job/gpu/cumulative_power", "Integral of GPU power usage", "ms * power"},
        {"user_job/gpu/cumulative_clocks_sm", "Integral of GPU frequency usage", "ms * frequency"},
        {"user_job/gpu/max_memory_used", "Maximum registered GPU memory usage", "bytes"},
        {"user_job/gpu/memory_total", "Total available GPU memory", "bytes"},

        {"codec/cpu/decode/*", "Time spent on decompressing data", "ms"},
        {"codec/cpu/encode/*/*", "Time spent on compressing data", "ms"},

        {"chunk_reader_statistics/data_bytes_read_from_disk", "Amount of chunk data read from disk", "bytes"},
        {"chunk_reader_statistics/data_bytes_read_from_cache", "Amount of chunk data read from cache", "bytes"},
        {"chunk_reader_statistics/data_bytes_transmitted", "Amount of chunk data transmitted over the network", "bytes"},
        {"chunk_reader_statistics/meta_bytes_read_from_disk", "Amount of chunk metadata read from disk", "bytes"},
        {"chunk_reader_statistics/wait_time", "Passive chunk reading duration, e.g. awaiting raw data from other nodes", "ms"},
        {"chunk_reader_statistics/read_time", "Active chunk reading duration, e.g. parsing rows from the raw data", "ms"},
        {"chunk_reader_statistics/idle_time", "Read data processing duration, during which the chunk reader is idle", "ms"},
    };

    return statisticsHints;
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

void BuildSupportedFeatures(TFluentMap fluent)
{
    const auto& descriptions = GetOperationStatisticsDescriptions();
    fluent
        .Item("operation_statistics_descriptions").DoMapFor(descriptions, [] (TFluentMap fluent, const TStatisticsDescription& description) {
            fluent.Item(description.Name).BeginMap()
                .Item("description").Value(description.Description)
                .Item("unit").Value(description.Unit)
            .EndMap();
        })
        .Item("scheduler_estimated_guarantee_attribute_name").Value("estimated_guarantee_resources");
}

////////////////////////////////////////////////////////////////////////////////

TString GuessGpuType(const TString& treeId)
{
    if (treeId.StartsWith("gpu_")) {
        return TString(TStringBuf(treeId).SubStr(4));
    }
    return "unknown";
}

std::vector<std::pair<TInstant, TInstant>> SplitTimeIntervalByHours(TInstant startTime, TInstant finishTime)
{
    YT_VERIFY(startTime <= finishTime);

    std::vector<std::pair<TInstant, TInstant>> timeIntervals;
    {
        i64 startTimeHours = startTime.Seconds() / 3600;
        i64 finishTimeHours = finishTime.Seconds() / 3600;
        TInstant currentStartTime = startTime;
        while (startTimeHours < finishTimeHours) {
            ++startTimeHours;
            auto hourBound = TInstant::Hours(startTimeHours);
            YT_VERIFY(currentStartTime <= hourBound);
            timeIntervals.push_back(std::pair(currentStartTime, hourBound));
            currentStartTime = hourBound;
        }
        YT_VERIFY(currentStartTime <= finishTime);
        if (currentStartTime < finishTime) {
            timeIntervals.push_back(std::pair(currentStartTime, finishTime));
        }
    }
    return timeIntervals;
}

////////////////////////////////////////////////////////////////////////////////

THashSet<int> GetDiskQuotaMedia(const TDiskQuota& diskQuota)
{
    THashSet<int> media;
    for (const auto& [index, _] : diskQuota.DiskSpacePerMedium) {
        media.insert(index);
    }
    return media;
}

////////////////////////////////////////////////////////////////////////////////

TYsonMapFragmentBatcher::TYsonMapFragmentBatcher(
    std::vector<NYson::TYsonString>* batchOutput,
    int maxBatchSize,
    EYsonFormat format)
    : BatchOutput_(batchOutput)
    , MaxBatchSize_(maxBatchSize)
    , BatchWriter_(CreateYsonWriter(&BatchStream_, format, EYsonType::MapFragment, /*enableRaw*/ false))
{ }

void TYsonMapFragmentBatcher::Flush()
{
    BatchWriter_->Flush();

    if (BatchStream_.empty()) {
        return;
    }

    BatchOutput_->push_back(TYsonString(BatchStream_.Str(), EYsonType::MapFragment));
    BatchSize_ = 0;
    BatchStream_.clear();
}

void TYsonMapFragmentBatcher::OnMyKeyedItem(TStringBuf key)
{
    BatchWriter_->OnKeyedItem(key);
    Forward(
        BatchWriter_.get(),
        /*onFinished*/ [&] {
            ++BatchSize_;
            if (BatchSize_ == MaxBatchSize_) {
                Flush();
            }
        });
}

////////////////////////////////////////////////////////////////////////////////

const std::vector<TSchedulerTreeAlertDescriptor>& GetSchedulerTreeAlertDescriptors()
{
    static const std::vector<TSchedulerTreeAlertDescriptor> SchedulerTreeAlertDescriptors = {
        TSchedulerTreeAlertDescriptor{
            .Type = ESchedulerAlertType::ManageSchedulingSegments,
            .Message = "Found errors during node scheduling segments management",
        },
    };

    return SchedulerTreeAlertDescriptors;
}

bool IsSchedulerTreeAlertType(ESchedulerAlertType alertType)
{
    for (const auto& [type, _] : GetSchedulerTreeAlertDescriptors()) {
        if (type == alertType) {
            return true;
        }
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
