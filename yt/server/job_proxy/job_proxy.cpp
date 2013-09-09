#include "stdafx.h"
#include "config.h"
#include "job_proxy.h"
#include "user_job.h"
#include "sorted_merge_job.h"
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
#include <core/concurrency/parallel_awaiter.h>

#include <core/misc/proc.h>
#include <core/misc/ref_counted_tracker.h>

#include <core/logging/log_manager.h>

#include <ytlib/scheduler/public.h>

#include <core/bus/tcp_client.h>

#include <core/rpc/bus_channel.h>

#include <server/scheduler/job_resources.h>

#include <ytlib/chunk_client/config.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/async_reader.h>

#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/node_tracker_client/node_directory.h>
#include <ytlib/node_tracker_client/helpers.h>

#include <ytlib/meta_state/master_channel.h>

// Defined in util/private/lf_alloc/lf_allocX64.cpp
void SetLargeBlockLimit(i64 limit);

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

static i64 RssLimitBoost = (i64) 1024 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    const TJobId& jobId)
    : Config(config)
    , JobId(jobId)
    , Logger(JobProxyLogger)
    , JobProxyMemoryLimit(InitialJobProxyMemoryLimit)
{
    Logger.AddTag(Sprintf("JobId: %s", ~ToString(JobId)));
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

    auto rsp = req->Invoke().Get();
    if (!rsp->IsOK()) {
        LOG_ERROR(*rsp, "Failed to get job spec");
        NLog::TLogManager::Get()->Shutdown();
        _exit(EJobProxyExitCode::HeartbeatFailed);
    }

    JobSpec = rsp->job_spec();
    ResourceUsage = rsp->resource_usage();

    LOG_INFO("Job spec received (JobType: %s, ResourceLimits: {%s})\n%s",
        ~NScheduler::EJobType(rsp->job_spec().type()).ToString(),
        ~FormatResources(ResourceUsage),
        ~rsp->job_spec().DebugString());

    JobProxyMemoryLimit = rsp->resource_usage().memory();
}

void TJobProxy::Run()
{
    auto result = DoRun();

    if (HeartbeatInvoker) {
        HeartbeatInvoker->Stop();
    }

    if (MemoryWatchdogInvoker) {
        MemoryWatchdogInvoker->Stop();
    }

    if (Job) {
        std::vector<NChunkClient::TChunkId> failedChunkIds;
        GetFailedChunks(&failedChunkIds).Get();
        LOG_INFO("Found %d failed chunks", static_cast<int>(failedChunkIds.size()));

        // For erasure chunks, replace part id with whole chunk id.
        auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        FOREACH (const auto& chunkId, failedChunkIds) {
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

    auto supervisorChannel = CreateBusChannel(supervisorClient, Config->SupervisorRpcTimeout);
    SupervisorProxy.reset(new TSupervisorServiceProxy(supervisorChannel));

    MasterChannel = CreateBusChannel(supervisorClient, Config->MasterRpcTimeout);

    RetrieveJobSpec();

    const auto& jobSpec = GetJobSpec();
    auto jobType = NScheduler::EJobType(jobSpec.type());

    const auto& schedulerJobSpecExt = jobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
    SetLargeBlockLimit(schedulerJobSpecExt.lfalloc_buffer_size());

    BlockCache = NChunkClient::CreateClientBlockCache(New<NChunkClient::TClientBlockCacheConfig>());

    NodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    NodeDirectory->MergeFrom(schedulerJobSpecExt.node_directory());

    HeartbeatInvoker = New<TPeriodicInvoker>(
        GetSyncInvoker(),
        BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)),
        Config->HeartbeatPeriod);

    MemoryWatchdogInvoker = New<TPeriodicInvoker>(
        GetSyncInvoker(),
        BIND(&TJobProxy::CheckMemoryUsage, MakeWeak(this)),
        Config->MemoryWatchdogPeriod);

    try {
        switch (jobType) {
            case NScheduler::EJobType::Map: {
                const auto& mapJobSpecExt = jobSpec.GetExtension(TMapJobSpecExt::map_job_spec_ext);
                auto userJobIO = CreateMapJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, mapJobSpecExt.mapper_spec(), std::move(userJobIO));
                JobProxyMemoryLimit -= mapJobSpecExt.mapper_spec().memory_reserve();
                break;
            }

            case NScheduler::EJobType::SortedReduce: {
                const auto& reduceJobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                auto userJobIO = CreateSortedReduceJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, reduceJobSpecExt.reducer_spec(), std::move(userJobIO));
                JobProxyMemoryLimit -= reduceJobSpecExt.reducer_spec().memory_reserve();
                break;
            }

            case NScheduler::EJobType::PartitionMap: {
                const auto& partitionJobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);
                YCHECK(partitionJobSpecExt.has_mapper_spec());
                auto userJobIO = CreatePartitionMapJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, partitionJobSpecExt.mapper_spec(), std::move(userJobIO));
                JobProxyMemoryLimit -= partitionJobSpecExt.mapper_spec().memory_reserve();
                break;
            }

            case NScheduler::EJobType::PartitionReduce: {
                const auto& reduceJobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
                auto userJobIO = CreatePartitionReduceJobIO(Config->JobIO, this);
                Job = CreateUserJob(this, reduceJobSpecExt.reducer_spec(), std::move(userJobIO));
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

            default:
                YUNREACHABLE();
        }

        MemoryWatchdogInvoker->Start();
        HeartbeatInvoker->Start();

        return Job->Run();
    } catch (const std::exception& ex) {
        LOG_ERROR(ex, "Job failed");

        TJobResult result;
        ToProto(result.mutable_error(), TError(ex));
        return result;
    }
}

TFuture<void> TJobProxy::GetFailedChunks(std::vector<NChunkClient::TChunkId>* failedChunks)
{
    *failedChunks = Job->GetFailedChunks();
     return MakeFuture();
/*
    if (failedChunks->empty()) {
        return MakeFuture();
    }

    yhash_set<NChunkClient::TChunkId> failedSet(failedChunks->begin(), failedChunks->end());

    auto awaiter = New<TParallelAwaiter>(GetSyncInvoker());

    LOG_INFO("Started collection additional failed chunks");

    FOREACH (const auto& inputSpec, JobSpec.input_specs()) {
        FOREACH (const auto& chunk, inputSpec.chunks()) {
            auto chunkId = NChunkClient::FromProto<TChunkId>(chunk.slice().chunk_id());
            auto pair = failedSet.insert(chunkId);
            if (pair.second) {
                LOG_INFO("Checking input chunk %s", ~ToString(chunkId));

                auto remoteReader = NChunkClient::CreateReplicationReader(
                    Config->JobIO->TableReader,
                    BlockCache,
                    MasterChannel,
                    chunkId,
                    FromProto<Stroka>(chunk.node_addresses()));

                awaiter->Await(
                    remoteReader->AsyncGetChunkMeta(),
                    BIND([=] (NChunkClient::IAsyncReader::TGetMetaResult result) mutable {
                        if (result.IsOK()) {
                            LOG_INFO("Input chunk %s is OK", ~ToString(chunkId));
                        } else {
                            LOG_ERROR(result, "Input chunk %s has failed", ~ToString(chunkId));
                            failedChunks->push_back(chunkId);
                        }
                        // This is important to capture #remoteReader.
                        remoteReader.Reset();
                    }));
            }
        }
    }

    return awaiter->Complete();
*/
}

void TJobProxy::ReportResult(const TJobResult& result)
{
    auto req = SupervisorProxy->OnJobFinished();
    ToProto(req->mutable_job_id(), JobId);
    *req->mutable_result() = result;

    auto rsp = req->Invoke().Get();
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
    LOG_DEBUG("Job proxy memory check (MemoryUsage: %" PRId64 ", MemoryLimit: %" PRId64 ")",
        memoryUsage,
        JobProxyMemoryLimit);
    if (memoryUsage > JobProxyMemoryLimit + RssLimitBoost) {
        LOG_FATAL(
            "Job proxy memory limit exceeded (MemoryUsage: %" PRId64 ", MemoryLimit: %" PRId64 ", RefCountedTracker: %s)",
            memoryUsage,
            JobProxyMemoryLimit,
            ~TRefCountedTracker::Get()->GetDebugInfo(2));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
