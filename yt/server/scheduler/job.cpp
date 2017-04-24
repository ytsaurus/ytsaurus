#include "private.h"
#include "job.h"
#include "exec_node.h"
#include "helpers.h"
#include "operation.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/scheduler/job.pb.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NNodeTrackerClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NYPath;
using namespace NJobTrackerClient;
using namespace NChunkClient::NProto;
using namespace NProto;
using namespace NProfiling;
using namespace NPhoenix;

////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////

void TBriefJobStatistics::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Timestamp);
    Persist(context, ProcessedInputRowCount);
    Persist(context, ProcessedInputUncompressedDataSize);
    Persist(context, ProcessedInputCompressedDataSize);
    Persist(context, ProcessedOutputRowCount);
    Persist(context, ProcessedOutputUncompressedDataSize);
    Persist(context, ProcessedOutputCompressedDataSize);
    Persist(context, InputPipeIdleTime);
    Persist(context, JobProxyCpuUsage);
}

////////////////////////////////////////////////////////////////////

void Serialize(const TBriefJobStatisticsPtr& briefJobStatistics, IYsonConsumer* consumer)
{
    if (!briefJobStatistics) {
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
        return;
    }

    BuildYsonFluently(consumer)
        .BeginAttributes()
            .Item("timestamp").Value(briefJobStatistics->Timestamp)
        .EndAttributes()
        .BeginMap()
            .Item("processed_input_row_count").Value(briefJobStatistics->ProcessedInputRowCount)
            .Item("processed_input_uncompressed_data_size").Value(briefJobStatistics->ProcessedInputUncompressedDataSize)
            .Item("processed_input_compressed_data_size").Value(briefJobStatistics->ProcessedInputCompressedDataSize)
            .Item("processed_output_uncompressed_data_size").Value(briefJobStatistics->ProcessedOutputUncompressedDataSize)
            .Item("processed_output_compressed_data_size").Value(briefJobStatistics->ProcessedOutputCompressedDataSize)
            .DoIf(static_cast<bool>(briefJobStatistics->InputPipeIdleTime), [&] (TFluentMap fluent) {
                fluent.Item("input_pipe_idle_time").Value(*(briefJobStatistics->InputPipeIdleTime));
            })
            .DoIf(static_cast<bool>(briefJobStatistics->JobProxyCpuUsage), [&] (TFluentMap fluent) {
                fluent.Item("job_proxy_cpu_usage").Value(*(briefJobStatistics->JobProxyCpuUsage));
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////

TJob::TJob(
    const TJobId& id,
    EJobType type,
    const TOperationId& operationId,
    TExecNodePtr node,
    TInstant startTime,
    const TJobResources& resourceLimits,
    bool interruptible,
    TJobSpecBuilder specBuilder)
    : Id_(id)
    , Type_(type)
    , OperationId_(operationId)
    , Node_(node)
    , StartTime_(startTime)
    , Interruptible_(interruptible)
    , State_(EJobState::None)
    , ResourceUsage_(resourceLimits)
    , ResourceLimits_(resourceLimits)
    , SpecBuilder_(std::move(specBuilder))
{ }

TDuration TJob::GetDuration() const
{
    return *FinishTime_ - StartTime_;
}

////////////////////////////////////////////////////////////////////

TJobSummary::TJobSummary()
{ }

TJobSummary::TJobSummary(const TJobPtr& job, TJobStatus* status)
    : Id(job->GetId())
    , State(job->GetState())
    , FinishTime(job->GetFinishTime())
    , ShouldLog(true)
{
    // TODO(ignat): it is hacky way, we should avoid it by saving status in controller.
    if (!status) {
        return;
    }

    Result = status->result();
    if (status->has_prepare_duration()) {
        PrepareDuration = FromProto<TDuration>(status->prepare_duration());
    }
    if (status->has_download_duration()) {
        DownloadDuration = FromProto<TDuration>(status->download_duration());
    }
    if (status->has_exec_duration()) {
        ExecDuration.Emplace();
        FromProto(ExecDuration.GetPtr(), status->exec_duration());
    }
    if (status->has_statistics()) {
        StatisticsYson = TYsonString(status->statistics());
    }
}

TJobSummary::TJobSummary(const TJobId& id, EJobState state)
    : Result()
    , Id(id)
    , State(state)
    , ShouldLog(false)
{ }

void TJobSummary::Persist(const TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Result);
    Persist(context, Id);
    Persist(context, State);
    Persist(context, FinishTime);
    Persist(context, PrepareDuration);
    Persist(context, DownloadDuration);
    Persist(context, ExecDuration);
    Persist(context, Statistics);
    Persist(context, StatisticsYson);
    Persist(context, ShouldLog);
}

////////////////////////////////////////////////////////////////////

TCompletedJobSummary::TCompletedJobSummary(const TJobPtr& job, TJobStatus* status, bool abandoned)
    : TJobSummary(job, status)
    , Abandoned(abandoned)
{
    const auto& schedulerResultExt = Result.GetExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
    InterruptReason = job->GetInterruptReason();
    YCHECK((InterruptReason == EInterruptReason::None && schedulerResultExt.unread_input_data_slice_descriptors_size() == 0) ||
        (InterruptReason != EInterruptReason::None && schedulerResultExt.unread_input_data_slice_descriptors_size() != 0));
}

void TCompletedJobSummary::Persist(const TPersistenceContext& context)
{
    TJobSummary::Persist(context);

    using NYT::Persist;

    Persist(context, Abandoned);
    Persist(context, InterruptReason);
    // TODO(max42): now we persist only those completed job summaries that correspond
    // to non-interrupted jobs, because Persist(context, UnreadInputDataSlices) produces
    // lots of ugly template resolution errors. I wasn't able to fix it :(
    YCHECK(InterruptReason == EInterruptReason::None);
    Persist(context, SplitJobCount);
}

////////////////////////////////////////////////////////////////////

TAbortedJobSummary::TAbortedJobSummary(const TJobPtr& job, TJobStatus* status)
    : TJobSummary(job, status)
    , AbortReason(GetAbortReason(Result))
{ }

TAbortedJobSummary::TAbortedJobSummary(const TJobId& id, EAbortReason abortReason)
    : TJobSummary(id, EJobState::Aborted)
    , AbortReason(abortReason)
{ }

TAbortedJobSummary::TAbortedJobSummary(const TJobSummary& other, EAbortReason abortReason)
    : TJobSummary(other)
    , AbortReason(abortReason)
{ }

////////////////////////////////////////////////////////////////////

TRunningJobSummary::TRunningJobSummary(const TJobPtr& job, TJobStatus* status)
    : TJobSummary(job, status)
    , Progress(status->progress())
{ }

////////////////////////////////////////////////////////////////////

TJobStatus JobStatusFromError(const TError& error)
{
    auto status = TJobStatus();
    ToProto(status.mutable_result()->mutable_error(), error);
    return status;
}

////////////////////////////////////////////////////////////////////

TJobStartRequest::TJobStartRequest(
    TJobId id,
    EJobType type,
    const TJobResources& resourceLimits,
    bool interruptible,
    TJobSpecBuilder specBuilder)
    : Id(id)
    , Type(type)
    , ResourceLimits(resourceLimits)
    , Interruptible(interruptible)
    , SpecBuilder(std::move(specBuilder))
{ }
    
////////////////////////////////////////////////////////////////////

void TScheduleJobResult::RecordFail(EScheduleJobFailReason reason)
{
    ++Failed[reason];
}

bool TScheduleJobResult::IsBackoffNeeded() const
{
    return
        !JobStartRequest &&
        Failed[EScheduleJobFailReason::NotEnoughResources] == 0 &&
        Failed[EScheduleJobFailReason::NoLocalJobs] == 0;
}

bool TScheduleJobResult::IsScheduleStopNeeded() const
{
    return
        Failed[EScheduleJobFailReason::NotEnoughChunkLists] > 0 ||
        Failed[EScheduleJobFailReason::JobSpecThrottling] > 0;
}

////////////////////////////////////////////////////////////////////

TJobId MakeJobId(NObjectClient::TCellTag tag, NNodeTrackerClient::TNodeId nodeId)
{
    return MakeId(
        EObjectType::SchedulerJob,
        tag,
        RandomNumber<ui64>(),
        nodeId);
}

NNodeTrackerClient::TNodeId NodeIdFromJobId(const TJobId& jobId)
{
    return jobId.Parts32[0];
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
