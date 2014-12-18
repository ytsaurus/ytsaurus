#include "stdafx.h"
#include "config.h"
#include "job_proxy.h"
#include "user_job.h"
#include "sorted_merge_job.h"
#include "remote_copy_job.h"
#include "merge_job.h"
#include "simple_sort_job.h"
#include "partition_sort_job.h"
#include "partition_job.h"
#include "map_job_io.h"
#include "partition_map_job_io.h"
#include "sorted_reduce_job_io.h"
#include "partition_reduce_job_io.h"
#include "user_job_io.h"

#include <core/actions/invoker_util.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/proc.h>
#include <core/misc/ref_counted_tracker.h>
#include <core/misc/lfalloc_helpers.h>

#include <core/ytree/convert.h>

#include <core/logging/log_manager.h>

#include <core/bus/tcp_client.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/helpers.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/cgroup/cgroup.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/chunk_reader.h>

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/scheduler/statistics.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/security_client/public.h>

#include <server/scheduler/job_resources.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NExecAgent;
using namespace NBus;
using namespace NRpc;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NCGroup;

////////////////////////////////////////////////////////////////////////////////

static const auto InitialJobProxyMemoryLimit = (i64) 100 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    const TJobId& jobId)
    : Config_(config)
    , JobId_(jobId)
    , Logger(JobProxyLogger)
    , JobThread_(New<TActionQueue>("JobMain"))
    , JobProxyMemoryLimit_(InitialJobProxyMemoryLimit)
{
    Logger.AddTag("JobId: %v", JobId_);
}

void TJobProxy::SendHeartbeat()
{
    auto req = SupervisorProxy_->OnJobProgress();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_progress(Job->GetProgress());
    ToProto(req->mutable_job_statistics(), Job->GetStatistics());

    req->Invoke().Subscribe(BIND(&TJobProxy::OnHeartbeatResponse, MakeWeak(this)));

    LOG_DEBUG("Supervisor heartbeat sent");
}

void TJobProxy::OnHeartbeatResponse(TSupervisorServiceProxy::TRspOnJobProgressPtr rsp)
{
    if (!rsp->IsOK()) {
        // NB: user process is not killed here.
        // Good user processes are supposed to die themselves
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        LOG_ERROR(*rsp, "Error sending heartbeat to supervisor");
        NLog::TLogManager::Get()->Shutdown();
        _exit(EJobProxyExitCode::HeartbeatFailed);
    }

    LOG_DEBUG("Successfully reported heartbeat to supervisor");
}

void TJobProxy::RetrieveJobSpec()
{
    LOG_INFO("Requesting job spec");

    auto req = SupervisorProxy_->GetJobSpec();
    ToProto(req->mutable_job_id(), JobId_);

    auto asyncRsp = req->Invoke();
    auto rsp = asyncRsp.Get();
    if (!rsp->IsOK()) {
        LOG_ERROR(*rsp, "Failed to get job spec");
        NLog::TLogManager::Get()->Shutdown();
        _exit(EJobProxyExitCode::HeartbeatFailed);
    }

    JobSpec_ = rsp->job_spec();
    ResourceUsage_ = rsp->resource_usage();

    LOG_INFO("Job spec received (JobType: %v, ResourceLimits: {%v})\n%v",
        NScheduler::EJobType(rsp->job_spec().type()),
        FormatResources(ResourceUsage_),
        rsp->job_spec().DebugString());

    JobProxyMemoryLimit_ = rsp->resource_usage().memory();
}

void TJobProxy::Run()
{
    auto result = BIND(&TJobProxy::DoRun, Unretained(this))
        .AsyncVia(JobThread_->GetInvoker())
        .Run().Get();

    if (HeartbeatExecutor_) {
        HeartbeatExecutor_->Stop();
    }

    if (MemoryWatchdogExecutor_) {
        MemoryWatchdogExecutor_->Stop();
    }

    if (Job) {
        auto failedChunkIds = Job->GetFailedChunkIds();
        LOG_INFO("Found %v failed chunks", static_cast<int>(failedChunkIds.size()));

        // For erasure chunks, replace part id with whole chunk id.
        auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        for (const auto& chunkId : failedChunkIds) {
            auto actualChunkId = IsErasureChunkPartId(chunkId)
                ? ErasureChunkIdFromPartId(chunkId)
                : chunkId;
            ToProto(schedulerResultExt->add_failed_chunk_ids(), actualChunkId);
        }

        auto jobStatistics = Job->GetStatistics();
        TStatistics customStatistics;
        if (jobStatistics.has_statistics()) {
            customStatistics = NYTree::ConvertTo<TStatistics>(NYTree::TYsonString(jobStatistics.statistics()));
        }

        if (Config_->ForceEnableAccounting) {
            TCpuAccounting cpuAccounting("");
            auto cpuStatistics = cpuAccounting.GetStatistics();
            AddStatistic(customStatistics, "/job_proxy/cpu", cpuStatistics);

            TBlockIO blockIO("");
            auto blockIOStatistics = blockIO.GetStatistics();
            AddStatistic(customStatistics, "/job_proxy/block_io", blockIOStatistics);
        }

        AddStatistic(customStatistics, "/job_proxy/input", jobStatistics.input());
        AddStatistic(customStatistics, "/job_proxy/output", jobStatistics.output());

        ToProto(jobStatistics.mutable_statistics(), NYTree::ConvertToYsonString(customStatistics).Data());
        ToProto(result.mutable_statistics(), jobStatistics);
    } else {
        ToProto(result.mutable_statistics(), ZeroJobStatistics());
    }

    ReportResult(result);
}

std::unique_ptr<TUserJobIO> TJobProxy::CreateUserJobIO()
{
    auto jobType = NScheduler::EJobType(JobSpec_.type());

    switch (jobType) {
        case NScheduler::EJobType::Map:
            return CreateMapJobIO(Config_->JobIO, this);

        case NScheduler::EJobType::SortedReduce:
            return CreateSortedReduceJobIO(Config_->JobIO, this);

        case NScheduler::EJobType::PartitionMap:
            return CreatePartitionMapJobIO(Config_->JobIO, this);

        case NScheduler::EJobType::ReduceCombiner:
        case NScheduler::EJobType::PartitionReduce:
            return CreatePartitionReduceJobIO(Config_->JobIO, this);

        default:
            YUNREACHABLE();
    }
}

TJobPtr TJobProxy::CreateBuiltinJob()
{
    auto jobType = NScheduler::EJobType(JobSpec_.type());
    switch (jobType) {
        case NScheduler::EJobType::OrderedMerge:
            return CreateOrderedMergeJob(this);

        case NScheduler::EJobType::UnorderedMerge:
            return CreateUnorderedMergeJob(this);

        case NScheduler::EJobType::SortedMerge:
            return CreateSortedMergeJob(this);

        case NScheduler::EJobType::PartitionSort:
            return CreatePartitionSortJob(this);

        case NScheduler::EJobType::SimpleSort:
            return CreateSimpleSortJob(this);

        case NScheduler::EJobType::Partition:
            return CreatePartitionJob(this);

        case NScheduler::EJobType::RemoteCopy:
            return CreateRemoteCopyJob(this);

        default:
            YUNREACHABLE();
    }

}

TJobResult TJobProxy::DoRun()
{
    auto supervisorClient = CreateTcpBusClient(Config_->SupervisorConnection);
    auto supervisorChannel = CreateBusChannel(supervisorClient);

    SupervisorProxy_.reset(new TSupervisorServiceProxy(supervisorChannel));
    SupervisorProxy_->SetDefaultTimeout(Config_->SupervisorRpcTimeout);

    MasterChannel_ = CreateAuthenticatedChannel(
        CreateRealmChannel(CreateBusChannel(supervisorClient), Config_->CellId),
        NSecurityClient::JobUserName);

    RetrieveJobSpec();

    const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    SetLargeBlockLimit(schedulerJobSpecExt.lfalloc_buffer_size());

    NodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();
    NodeDirectory_->MergeFrom(schedulerJobSpecExt.node_directory());

    HeartbeatExecutor_ = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)),
        Config_->HeartbeatPeriod);

    if (schedulerJobSpecExt.job_proxy_memory_control()) {
        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TJobProxy::CheckMemoryUsage, MakeWeak(this)),
            Config_->MemoryWatchdogPeriod);
    }

    try {
        if (schedulerJobSpecExt.has_user_job_spec()) {
            auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
            JobProxyMemoryLimit_ -= userJobSpec.memory_reserve();
            auto jobIO = CreateUserJobIO();
            Job = CreateUserJob(this, userJobSpec, std::move(jobIO), JobId_);
        } else {
            Job = CreateBuiltinJob();
        }


        if (MemoryWatchdogExecutor_) {
            MemoryWatchdogExecutor_->Start();
        }
        HeartbeatExecutor_->Start();

        return Job->Run();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Job failed");

        TJobResult result;
        ToProto(result.mutable_error(), TError(ex));
        return result;
    }
}

void TJobProxy::ReportResult(const TJobResult& result)
{
    auto req = SupervisorProxy_->OnJobFinished();
    ToProto(req->mutable_job_id(), JobId_);
    *req->mutable_result() = result;

    auto asyncRsp = req->Invoke();
    auto rsp = asyncRsp.Get();
    if (!rsp->IsOK()) {
        LOG_ERROR(*rsp, "Failed to report job result");
        NLog::TLogManager::Get()->Shutdown();
        _exit(EJobProxyExitCode::ResultReportFailed);
    }
}

TJobProxyConfigPtr TJobProxy::GetConfig()
{
    return Config_;
}

const TJobSpec& TJobProxy::GetJobSpec() const
{
    return JobSpec_;
}

const TNodeResources& TJobProxy::GetResourceUsage() const
{
    return ResourceUsage_;
}

void TJobProxy::SetResourceUsage(const TNodeResources& usage)
{
    ResourceUsage_ = usage;

    // Fire-and-forget.
    auto req = SupervisorProxy_->UpdateResourceUsage();
    ToProto(req->mutable_job_id(), JobId_);
    *req->mutable_resource_usage() = ResourceUsage_;
    req->Invoke().Subscribe(BIND(&TJobProxy::OnResourcesUpdated, MakeWeak(this)));
}

void TJobProxy::OnResourcesUpdated(TSupervisorServiceProxy::TRspUpdateResourceUsagePtr rsp)
{
    if (!rsp->IsOK()) {
        LOG_ERROR(*rsp, "Failed to update resource usage");
        NLog::TLogManager::Get()->Shutdown();
        _exit(EJobProxyExitCode::ResourcesUpdateFailed);
    }

    LOG_DEBUG("Successfully updated resource usage");
}

void TJobProxy::ReleaseNetwork()
{
    auto usage = GetResourceUsage();
    usage.set_network(0);
    SetResourceUsage(usage);
}

IChannelPtr TJobProxy::GetMasterChannel() const
{
    return MasterChannel_;
}

IBlockCachePtr TJobProxy::GetCompressedBlockCache() const
{
    return GetNullBlockCache();
}

IBlockCachePtr TJobProxy::GetUncompressedBlockCache() const
{
    return GetNullBlockCache();
}

TNodeDirectoryPtr TJobProxy::GetNodeDirectory() const
{
    return NodeDirectory_;
}

void TJobProxy::CheckMemoryUsage()
{
    auto memoryUsage = GetProcessRss();

    LOG_DEBUG("Job proxy memory check (MemoryUsage: %v, MemoryLimit: %v)",
        memoryUsage,
        JobProxyMemoryLimit_);

    LOG_DEBUG("LFAlloc counters (LargeBlocks: %v, SmallBlocks: %v, System: %v, Used: %v, Mmapped: %v)",
        NLFAlloc::GetCurrentLargeBlocks(),
        NLFAlloc::GetCurrentSmallBlocks(),
        NLFAlloc::GetCurrentSystem(),
        NLFAlloc::GetCurrentUsed(),
        NLFAlloc::GetCurrentMmapped());

    if (memoryUsage > JobProxyMemoryLimit_) {
        LOG_FATAL("Job proxy memory limit exceeded (MemoryUsage: %v, MemoryLimit: %v, RefCountedTracker: %v)",
            memoryUsage,
            JobProxyMemoryLimit_,
            TRefCountedTracker::Get()->GetDebugInfo(2));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
