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

#include <core/logging/log_manager.h>

#include <ytlib/scheduler/public.h>

#include <core/bus/tcp_client.h>

#include <core/rpc/bus_channel.h>

#include <server/scheduler/job_resources.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/reader.h>

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/hydra/peer_channel.h>

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

////////////////////////////////////////////////////////////////////////////////

static i64 InitialJobProxyMemoryLimit = (i64) 100 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    const TJobId& jobId)
    : Config(config)
    , JobId(jobId)
    , Logger(JobProxyLogger)
    , JobThread(New<TActionQueue>("JobMain"))
    , JobProxyMemoryLimit(InitialJobProxyMemoryLimit)
{
    Logger.AddTag("JobId: %v", JobId);
}

void TJobProxy::SendHeartbeat()
{
    auto req = SupervisorProxy->OnJobProgress();
    ToProto(req->mutable_job_id(), JobId);
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

    auto req = SupervisorProxy->GetJobSpec();
    ToProto(req->mutable_job_id(), JobId);

    auto asyncRsp = req->Invoke();
    auto rsp = asyncRsp.Get();
    if (!rsp->IsOK()) {
        LOG_ERROR(*rsp, "Failed to get job spec");
        NLog::TLogManager::Get()->Shutdown();
        _exit(EJobProxyExitCode::HeartbeatFailed);
    }

    JobSpec = rsp->job_spec();
    ResourceUsage = rsp->resource_usage();

    LOG_INFO("Job spec received (JobType: %v, ResourceLimits: {%v})\n%v",
        NScheduler::EJobType(rsp->job_spec().type()),
        FormatResources(ResourceUsage),
        rsp->job_spec().DebugString());

    JobProxyMemoryLimit = rsp->resource_usage().memory();
}

void TJobProxy::Run()
{
    auto result = BIND(&TJobProxy::DoRun, Unretained(this))
        .AsyncVia(JobThread->GetInvoker())
        .Run().Get();

    if (HeartbeatExecutor) {
        HeartbeatExecutor->Stop();
    }

    if (MemoryWatchdogExecutor) {
        MemoryWatchdogExecutor->Stop();
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

        ToProto(result.mutable_statistics(), Job->GetStatistics());
    } else {
        ToProto(result.mutable_statistics(), ZeroJobStatistics());
    }

    ReportResult(result);
}

TJobResult TJobProxy::DoRun()
{
    auto supervisorClient = CreateTcpBusClient(Config->SupervisorConnection);
    auto supervisorChannel = CreateBusChannel(supervisorClient);
    
    SupervisorProxy.reset(new TSupervisorServiceProxy(supervisorChannel));
    SupervisorProxy->SetDefaultTimeout(Config->SupervisorRpcTimeout);

    MasterChannel = CreateBusChannel(supervisorClient);

    RetrieveJobSpec();

    const auto& jobSpec = GetJobSpec();
    auto jobType = NScheduler::EJobType(jobSpec.type());

    const auto& schedulerJobSpecExt = jobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    SetLargeBlockLimit(schedulerJobSpecExt.lfalloc_buffer_size());

    BlockCache = NChunkClient::CreateClientBlockCache(New<NChunkClient::TClientBlockCacheConfig>());

    NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    NodeDirectory->MergeFrom(schedulerJobSpecExt.node_directory());

    HeartbeatExecutor = New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)),
        Config->HeartbeatPeriod);

    if (schedulerJobSpecExt.job_proxy_memory_control()) {
        MemoryWatchdogExecutor = New<TPeriodicExecutor>(
            GetSyncInvoker(),
            BIND(&TJobProxy::CheckMemoryUsage, MakeWeak(this)),
            Config->MemoryWatchdogPeriod);
    }

    try {
        switch (jobType) {
            case NScheduler::EJobType::Map: {
                const auto& mapJobSpecExt = jobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
                auto userJobIO = CreateMapJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, mapJobSpecExt.mapper_spec(), std::move(userJobIO), JobId);
                JobProxyMemoryLimit -= mapJobSpecExt.mapper_spec().memory_reserve();
                break;
            }

            case NScheduler::EJobType::SortedReduce: {
                const auto& reduceJobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                auto userJobIO = CreateSortedReduceJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, reduceJobSpecExt.reducer_spec(), std::move(userJobIO), JobId);
                JobProxyMemoryLimit -= reduceJobSpecExt.reducer_spec().memory_reserve();
                break;
            }

            case NScheduler::EJobType::PartitionMap: {
                const auto& partitionJobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
                YCHECK(partitionJobSpecExt.has_mapper_spec());
                auto userJobIO = CreatePartitionMapJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, partitionJobSpecExt.mapper_spec(), std::move(userJobIO), JobId);
                JobProxyMemoryLimit -= partitionJobSpecExt.mapper_spec().memory_reserve();
                break;
            }

            case NScheduler::EJobType::ReduceCombiner: 
            case NScheduler::EJobType::PartitionReduce: {
                const auto& reduceJobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                auto userJobIO = CreatePartitionReduceJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, reduceJobSpecExt.reducer_spec(), std::move(userJobIO), JobId);
                JobProxyMemoryLimit -= reduceJobSpecExt.reducer_spec().memory_reserve();
                break;
            }

            case NScheduler::EJobType::OrderedMerge:
                Job = CreateOrderedMergeJob(this);
                break;

            case NScheduler::EJobType::UnorderedMerge:
                Job = CreateUnorderedMergeJob(this);
                break;

            case NScheduler::EJobType::SortedMerge:
                Job = CreateSortedMergeJob(this);
                break;

            case NScheduler::EJobType::PartitionSort:
                Job = CreatePartitionSortJob(this);
                break;

            case NScheduler::EJobType::SimpleSort:
                Job = CreateSimpleSortJob(this);
                break;

            case NScheduler::EJobType::Partition:
                Job = CreatePartitionJob(this);
                break;
            
            case NScheduler::EJobType::RemoteCopy:
                Job = CreateRemoteCopyJob(this);
                break;

            default:
                YUNREACHABLE();
        }

        if (MemoryWatchdogExecutor) {
	        MemoryWatchdogExecutor->Start();
        }
        HeartbeatExecutor->Start();

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
    auto req = SupervisorProxy->OnJobFinished();
    ToProto(req->mutable_job_id(), JobId);
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
    return Config;
}

const TJobSpec& TJobProxy::GetJobSpec() const
{
    return JobSpec;
}

const TNodeResources& TJobProxy::GetResourceUsage() const
{
    return ResourceUsage;
}

void TJobProxy::SetResourceUsage(const TNodeResources& usage)
{
    ResourceUsage = usage;

    // Fire-and-forget.
    auto req = SupervisorProxy->UpdateResourceUsage();
    ToProto(req->mutable_job_id(), JobId);
    *req->mutable_resource_usage() = ResourceUsage;
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
    return MasterChannel;
}

IBlockCachePtr TJobProxy::GetBlockCache() const
{
    return BlockCache;
}

TNodeDirectoryPtr TJobProxy::GetNodeDirectory() const
{
    return NodeDirectory;
}

void TJobProxy::CheckMemoryUsage()
{
    auto memoryUsage = GetProcessRss();
    LOG_DEBUG("Job proxy memory check (MemoryUsage: %v" ", MemoryLimit: %v" ")",
        memoryUsage,
        JobProxyMemoryLimit);
    LOG_DEBUG("lf_alloc counters (LargeBlocks: %v" ", SmallBlocks: %v" ", System: %v" ", Used: %v" ", Mmapped: %v" ")",
            NLFAlloc::GetCurrentLargeBlocks(),
            NLFAlloc::GetCurrentSmallBlocks(),
            NLFAlloc::GetCurrentSystem(),
            NLFAlloc::GetCurrentUsed(),
            NLFAlloc::GetCurrentMmapped());

    if (memoryUsage > JobProxyMemoryLimit) {
        LOG_FATAL(
            "Job proxy memory limit exceeded (MemoryUsage: %v" ", MemoryLimit: %v" ", RefCountedTracker: %v)",
            memoryUsage,
            JobProxyMemoryLimit,
            TRefCountedTracker::Get()->GetDebugInfo(2));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
