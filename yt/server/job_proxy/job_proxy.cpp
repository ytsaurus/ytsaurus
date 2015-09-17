
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
#include "job_prober_service.h"

#include <core/concurrency/action_queue.h>
#include <core/concurrency/periodic_executor.h>

#include <core/misc/proc.h>
#include <core/misc/ref_counted_tracker.h>
#include <core/misc/lfalloc_helpers.h>

#include <core/logging/log_manager.h>

#include <core/bus/tcp_client.h>
#include <core/bus/tcp_server.h>

#include <core/rpc/bus_channel.h>
#include <core/rpc/server.h>
#include <core/rpc/helpers.h>

#include <core/ytree/public.h>

#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>

#include <ytlib/scheduler/public.h>

#include <ytlib/cgroup/cgroup.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/data_statistics.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NExecAgent;
using namespace NBus;
using namespace NRpc;
using namespace NApi;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NCGroup;
using namespace NYTree;

using NJobTrackerClient::TStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto InitialJobProxyMemoryLimit = (i64) 100 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    INodePtr configNode,
    const TJobId& jobId)
    : ConfigNode_(configNode)
    , JobId_(jobId)
    , Logger(JobProxyLogger)
    , JobProxyMemoryLimit_(InitialJobProxyMemoryLimit)
    , JobThread_(New<TActionQueue>("JobMain"))
    , ControlThread_(New<TActionQueue>("Control"))
{
    Logger.AddTag("JobId: %v", JobId_);
}

std::vector<NChunkClient::TChunkId> TJobProxy::DumpInputContext(const TJobId& jobId)
{
    ValidateJobId(jobId);
    return Job_->DumpInputContext();
}

NYTree::TYsonString TJobProxy::Strace(const TJobId& jobId)
{
    ValidateJobId(jobId);
    return Job_->Strace();
}

void TJobProxy::ValidateJobId(const TJobId& jobId)
{
    if (JobId_ != jobId) {
        THROW_ERROR_EXCEPTION("Job id mismatch: expected %v, got %v",
            JobId_,
            jobId);
    }

    if (!Job_) {
        THROW_ERROR_EXCEPTION("Job has not started yet");
    }
}

void TJobProxy::SendHeartbeat()
{
    auto req = SupervisorProxy_->OnJobProgress();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_progress(Job_->GetProgress());
    ToProto(req->mutable_statistics(), GetStatistics());

    req->Invoke().Subscribe(BIND(&TJobProxy::OnHeartbeatResponse, MakeWeak(this)));

    LOG_DEBUG("Supervisor heartbeat sent");
}

void TJobProxy::OnHeartbeatResponse(const TError& error)
{
    if (!error.IsOK()) {
        // NB: user process is not killed here.
        // Good user processes are supposed to die themselves
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        LOG_ERROR(error, "Error sending heartbeat to supervisor");
        Exit(EJobProxyExitCode::HeartbeatFailed);
    }

    LOG_DEBUG("Successfully reported heartbeat to supervisor");
}

void TJobProxy::RetrieveJobSpec()
{
    LOG_INFO("Requesting job spec");

    auto req = SupervisorProxy_->GetJobSpec();
    ToProto(req->mutable_job_id(), JobId_);

    auto rspOrError = req->Invoke().Get();
    if (!rspOrError.IsOK()) {
        LOG_ERROR(rspOrError, "Failed to get job spec");
        Exit(EJobProxyExitCode::HeartbeatFailed);
    }

    const auto& rsp = rspOrError.Value();
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
    auto resultOrError = BIND(&TJobProxy::DoRun, Unretained(this))
        .AsyncVia(JobThread_->GetInvoker())
        .Run()
        .Get();

    TJobResult result;
    if (!resultOrError.IsOK()) {
        LOG_ERROR(resultOrError, "Job failed");
        ToProto(result.mutable_error(), resultOrError);
    } else {
        result = resultOrError.Value();
    }

    if (HeartbeatExecutor_) {
        HeartbeatExecutor_->Stop();
    }

    if (MemoryWatchdogExecutor_) {
        MemoryWatchdogExecutor_->Stop();
    }

    if (Job_) {
        auto failedChunkIds = Job_->GetFailedChunkIds();
        LOG_INFO("Found %v failed chunks", static_cast<int>(failedChunkIds.size()));

        // For erasure chunks, replace part id with whole chunk id.
        auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        for (const auto& chunkId : failedChunkIds) {
            auto actualChunkId = IsErasureChunkPartId(chunkId)
                ? ErasureChunkIdFromPartId(chunkId)
                : chunkId;
            ToProto(schedulerResultExt->add_failed_chunk_ids(), actualChunkId);
        }

        ToProto(result.mutable_statistics(), GetStatistics());
    } else {
        result.mutable_statistics();
    }

    ReportResult(result);
}

std::unique_ptr<IUserJobIO> TJobProxy::CreateUserJobIO()
{
    auto jobType = NScheduler::EJobType(JobSpec_.type());

    switch (jobType) {
        case NScheduler::EJobType::Map:
            return CreateMapJobIO(this);

        case NScheduler::EJobType::OrderedMap:
            return CreateOrderedMapJobIO(this);

        case NScheduler::EJobType::SortedReduce:
            return CreateSortedReduceJobIO(this);

        case NScheduler::EJobType::PartitionMap:
            return CreatePartitionMapJobIO(this);

        // ToDo(psushin): handle separately to form job result differently.
        case NScheduler::EJobType::ReduceCombiner:
        case NScheduler::EJobType::PartitionReduce:
            return CreatePartitionReduceJobIO(this);

        default:
            YUNREACHABLE();
    }
}

IJobPtr TJobProxy::CreateBuiltinJob()
{
    auto jobType = NScheduler::EJobType(JobSpec_.type());
    switch (jobType) {
        case NScheduler::EJobType::OrderedMerge:
            return CreateOrderedMergeJob(this);

        case NScheduler::EJobType::UnorderedMerge:
            return CreateUnorderedMergeJob(this);

        case NScheduler::EJobType::SortedMerge:
            return CreateSortedMergeJob(this);

        case NScheduler::EJobType::FinalSort:
        case NScheduler::EJobType::IntermediateSort:
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
    try {
        Config_->Load(ConfigNode_);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error parsing job proxy configuration")
            << ex;
    }

    RpcServer = CreateBusServer(CreateTcpBusServer(Config_->RpcServer));
    RpcServer->RegisterService(CreateJobProberService(this));
    RpcServer->Start();

    auto supervisorClient = CreateTcpBusClient(Config_->SupervisorConnection);
    auto supervisorChannel = CreateBusChannel(supervisorClient);

    SupervisorProxy_.reset(new TSupervisorServiceProxy(supervisorChannel));
    SupervisorProxy_->SetDefaultTimeout(Config_->SupervisorRpcTimeout);

    auto clusterConnection = CreateConnection(Config_->ClusterConnection);

    TClientOptions clientOptions;
    clientOptions.User = NSecurityClient::JobUserName;
    Client_ = clusterConnection->CreateClient(clientOptions);

    RetrieveJobSpec();

    const auto& schedulerJobSpecExt = JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    SetLargeBlockLimit(schedulerJobSpecExt.lfalloc_buffer_size());
    EnableJobProxyMemoryControl_ = schedulerJobSpecExt.enable_job_proxy_memory_control();

    NodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();
    NodeDirectory_->MergeFrom(schedulerJobSpecExt.node_directory());

    HeartbeatExecutor_ = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)),
        Config_->HeartbeatPeriod);

    MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TJobProxy::CheckMemoryUsage, MakeWeak(this)),
        Config_->MemoryWatchdogPeriod);

    if (schedulerJobSpecExt.has_user_job_spec()) {
        auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
        JobProxyMemoryLimit_ -= userJobSpec.memory_reserve();
        Job_ = CreateUserJob(
            this,
            userJobSpec,
            JobId_,
            CreateUserJobIO());
    } else {
        Job_ = CreateBuiltinJob();
    }

    MemoryWatchdogExecutor_->Start();
    HeartbeatExecutor_->Start();

    return Job_->Run();
}

void TJobProxy::ReportResult(const TJobResult& result)
{
    auto req = SupervisorProxy_->OnJobFinished();
    ToProto(req->mutable_job_id(), JobId_);
    *req->mutable_result() = result;

    auto rspOrError = req->Invoke().Get();
    if (!rspOrError.IsOK()) {
        LOG_ERROR(rspOrError, "Failed to report job result");
        Exit(EJobProxyExitCode::ResultReportFailed);
    }
}

TStatistics TJobProxy::GetStatistics() const
{
    YCHECK(Job_);
    auto statistics = Job_->GetStatistics();

    if (Config_->IsCGroupSupported(TCpuAccounting::Name)) {
        auto cpuAccounting = GetCurrentCGroup<TCpuAccounting>();
        auto cpuStatistics = cpuAccounting.GetStatistics();
        statistics.AddSample("/job_proxy/cpu", cpuStatistics);
    }

    if (Config_->IsCGroupSupported(TBlockIO::Name)) {
        auto blockIO = GetCurrentCGroup<TBlockIO>();
        auto blockIOStatistics = blockIO.GetStatistics();
        statistics.AddSample("/job_proxy/block_io", blockIOStatistics);
    }

    statistics.AddSample("/job_proxy/max_memory", MaxMemoryUsage_);
    statistics.AddSample("/job_proxy/memory_hog", MaxMemoryUsage_ > JobProxyMemoryLimit_ ? 1 : 0);

    return statistics;
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

void TJobProxy::OnResourcesUpdated(const TError& error)
{
    if (!error.IsOK()) {
        LOG_ERROR(error, "Failed to update resource usage");
        Exit(EJobProxyExitCode::ResourcesUpdateFailed);
    }

    LOG_DEBUG("Successfully updated resource usage");
}

void TJobProxy::ReleaseNetwork()
{
    auto usage = GetResourceUsage();
    usage.set_network(0);
    SetResourceUsage(usage);
}

NApi::IClientPtr TJobProxy::GetClient() const
{
    return Client_;
}

IBlockCachePtr TJobProxy::GetBlockCache() const
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
    MaxMemoryUsage_ = std::max(MaxMemoryUsage_.load(), memoryUsage);

    LOG_DEBUG("Job proxy memory check (MemoryUsage: %v, MemoryLimit: %v)",
        memoryUsage,
        JobProxyMemoryLimit_);

    LOG_DEBUG("LFAlloc counters (LargeBlocks: %v, SmallBlocks: %v, System: %v, Used: %v, Mmapped: %v)",
        NLFAlloc::GetCurrentLargeBlocks(),
        NLFAlloc::GetCurrentSmallBlocks(),
        NLFAlloc::GetCurrentSystem(),
        NLFAlloc::GetCurrentUsed(),
        NLFAlloc::GetCurrentMmapped());

    if (EnableJobProxyMemoryControl_ && memoryUsage > JobProxyMemoryLimit_) {
        LOG_FATAL("Job proxy memory limit exceeded (MemoryUsage: %v, MemoryLimit: %v, RefCountedTracker: %v)",
            memoryUsage,
            JobProxyMemoryLimit_,
            TRefCountedTracker::Get()->GetDebugInfo(2));
    }
}

void TJobProxy::Exit(EJobProxyExitCode exitCode)
{
    if (Job_) {
        Job_->Abort();
    }
    NLogging::TLogManager::Get()->Shutdown();
    _exit(static_cast<int>(exitCode));
}

NLogging::TLogger TJobProxy::GetLogger() const
{
    return Logger;
}

IInvokerPtr TJobProxy::GetControlInvoker() const
{
    return ControlThread_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
