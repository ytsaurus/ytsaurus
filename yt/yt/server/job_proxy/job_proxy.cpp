#include "job_proxy.h"
#include "cpu_monitor.h"
#include "merge_job.h"
#include "partition_job.h"
#include "partition_sort_job.h"
#include "remote_copy_job.h"
#include "simple_sort_job.h"
#include "sorted_merge_job.h"
#include "user_job.h"
#include "user_job_write_controller.h"
#include "job_prober_service.h"
#include "job_throttler.h"

#include <yt/yt/server/lib/containers/public.h>

#include <yt/yt/server/lib/exec_agent/config.h>
#include <yt/yt/server/lib/exec_agent/proto/supervisor_service.pb.h>

#include <yt/yt/server/lib/user_job_synchronizer_client/user_job_synchronizer.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/yt/ytlib/job_tracker_client/statistics.h>

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <util/system/fs.h>
#include <util/system/execpath.h>

#include <util/folder/dirut.h>

namespace NYT::NJobProxy {

using namespace NScheduler;
using namespace NExecAgent;
using namespace NExecAgent::NProto;
using namespace NBus;
using namespace NRpc;
using namespace NApi;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NJobProber;
using namespace NJobProberClient;
using namespace NJobProxy;
using namespace NJobTrackerClient;
using namespace NJobTrackerClient::NProto;
using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NContainers;

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyConfigPtr config,
    TOperationId operationId,
    TJobId jobId)
    : Config_(std::move(config))
    , OperationId_(operationId)
    , JobId_(jobId)
    , JobThread_(New<TActionQueue>("JobMain"))
    , ControlThread_(New<TActionQueue>("Control"))
    , Logger(JobProxyLogger.WithTag("OperationId: %v, JobId: %v",
        OperationId_,
        JobId_))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }
}

TString TJobProxy::GetPreparationPath() const
{
    return NFs::CurrentWorkingDirectory();
}

TString TJobProxy::GetSlotPath() const
{
    if (!Config_->RootPath || Config_->TestRootFS) {
        return NFs::CurrentWorkingDirectory();
    }

    return "/slot";
}

std::vector<NChunkClient::TChunkId> TJobProxy::DumpInputContext()
{
    auto job = GetJobOrThrow();
    return job->DumpInputContext();
}

TString TJobProxy::GetStderr()
{
    auto job = GetJobOrThrow();
    return job->GetStderr();
}

TYsonString TJobProxy::PollJobShell(
    const TJobShellDescriptor& jobShellDescriptor,
    const TYsonString& parameters)
{
    auto job = GetJobOrThrow();
    return job->PollJobShell(jobShellDescriptor, parameters);
}

void TJobProxy::Interrupt()
{
    auto job = GetJobOrThrow();
    job->Interrupt();
}

void TJobProxy::Fail()
{
    auto job = GetJobOrThrow();
    job->Fail();
}

IServerPtr TJobProxy::GetRpcServer() const
{
    return RpcServer_;
}

TTrafficMeterPtr TJobProxy::GetTrafficMeter() const
{
    return TrafficMeter_;
}

IThroughputThrottlerPtr TJobProxy::GetInBandwidthThrottler() const
{
    return InBandwidthThrottler_;
}

IThroughputThrottlerPtr TJobProxy::GetOutBandwidthThrottler() const
{
    return OutBandwidthThrottler_;
}

IThroughputThrottlerPtr TJobProxy::GetOutRpsThrottler() const
{
    return OutRpsThrottler_;
}

void TJobProxy::SendHeartbeat()
{
    auto job = FindJob();
    if (!job) {
        return;
    }

    auto req = SupervisorProxy_->OnJobProgress();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_progress(job->GetProgress());
    req->set_statistics(ConvertToYsonString(GetStatistics()).ToString());
    req->set_stderr_size(job->GetStderrSize());

    req->Invoke().Subscribe(BIND(&TJobProxy::OnHeartbeatResponse, MakeWeak(this)));

    YT_LOG_DEBUG("Supervisor heartbeat sent");
}

void TJobProxy::OnHeartbeatResponse(const TError& error)
{
    if (!error.IsOK()) {
        // NB: user process is not killed here.
        // Good user processes are supposed to die themselves
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        YT_LOG_ERROR(error, "Error sending heartbeat to supervisor");
        Exit(EJobProxyExitCode::HeartbeatFailed);
    }

    YT_LOG_DEBUG("Successfully reported heartbeat to supervisor");
}

void TJobProxy::LogJobSpec(TJobSpec jobSpec)
{
    // We patch copy of job spec for better reading experience.
    if (jobSpec.HasExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext)) {
        auto* schedulerJobSpecExt = jobSpec.MutableExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        // Some fields are serialized in binary YSON, let's make them human readable.
        #define TRANSFORM_TO_PRETTY_YSON(object, field) \
            if (object->has_ ## field() && !object->field().empty()) { \
                object->set_ ## field(ConvertToYsonString(TYsonString(object->field()), EYsonFormat::Pretty).ToString()); \
            }
        TRANSFORM_TO_PRETTY_YSON(schedulerJobSpecExt, io_config);
        TRANSFORM_TO_PRETTY_YSON(schedulerJobSpecExt, table_reader_options);
        TRANSFORM_TO_PRETTY_YSON(schedulerJobSpecExt, acl);
        TRANSFORM_TO_PRETTY_YSON(schedulerJobSpecExt, job_cpu_monitor_config);

        // Input table specs may be large, so we print them separately.
        NProtoBuf::RepeatedPtrField<TTableInputSpec> primaryInputTableSpecs;
        schedulerJobSpecExt->mutable_input_table_specs()->Swap(&primaryInputTableSpecs);

        NProtoBuf::RepeatedPtrField<TTableInputSpec> foreignInputTableSpecs;
        schedulerJobSpecExt->mutable_foreign_input_table_specs()->Swap(&foreignInputTableSpecs);

        for (const auto& [name, inputTableSpecs] : {
            std::make_pair("primary", &primaryInputTableSpecs),
            std::make_pair("foreign", &foreignInputTableSpecs)
        })
        {
            TStringBuilder builder;
            for (const auto& tableSpec : *inputTableSpecs) {
                builder.AppendString(tableSpec.DebugString());
                builder.AppendChar('\n');
            }
            YT_LOG_DEBUG("Job spec %v input table specs\n%v", name, builder.Flush());
        }

        // Node directory is huge and useless when debugging.
        schedulerJobSpecExt->clear_input_node_directory();
    }
    YT_LOG_DEBUG("Job spec debug string\n%v", jobSpec.DebugString());
}

void TJobProxy::RetrieveJobSpec()
{
    YT_LOG_INFO("Requesting job spec");

    auto req = SupervisorProxy_->GetJobSpec();
    ToProto(req->mutable_job_id(), JobId_);

    auto rspOrError = req->Invoke().Get();
    if (!rspOrError.IsOK()) {
        YT_LOG_ERROR(rspOrError, "Failed to get job spec");
        Exit(EJobProxyExitCode::GetJobSpecFailed);
    }

    const auto& rsp = rspOrError.Value();

    if (rsp->job_spec().version() != GetJobSpecVersion()) {
        YT_LOG_WARNING("Invalid job spec version (Expected: %v, Actual: %v)",
            GetJobSpecVersion(),
            rsp->job_spec().version());
        Exit(EJobProxyExitCode::InvalidSpecVersion);
    }

    JobSpecHelper_ = CreateJobSpecHelper(rsp->job_spec());
    const auto& resourceUsage = rsp->resource_usage();

    Ports_ = FromProto<std::vector<int>>(rsp->ports());

    auto authenticatedUser = GetJobSpecHelper()->GetSchedulerJobSpecExt().authenticated_user();
    YT_LOG_INFO("Job spec received (JobType: %v, AuthenticatedUser: %v, ResourceLimits: {Cpu: %v, Memory: %v, Network: %v})",
        NScheduler::EJobType(rsp->job_spec().type()),
        authenticatedUser,
        resourceUsage.cpu(),
        resourceUsage.memory(),
        resourceUsage.network());

    // Job spec is copied intentionally.
    LogJobSpec(rsp->job_spec());

    auto totalMemoryReserve = resourceUsage.memory();
    CpuGuarantee_ = resourceUsage.cpu();
    NetworkUsage_ = resourceUsage.network();

    // We never report to node less memory usage, than was initially reserved.
    TotalMaxMemoryUsage_ = totalMemoryReserve - Config_->AheadMemoryReserve;
    ApprovedMemoryReserve_ = totalMemoryReserve;
    RequestedMemoryReserve_ = totalMemoryReserve;

    auto schedulerJobSpecExt = JobSpecHelper_->GetSchedulerJobSpecExt();
    if (schedulerJobSpecExt.has_user_job_spec()) {
        const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
        JobProxyMemoryReserve_ = totalMemoryReserve - userJobSpec.memory_reserve();
        YT_LOG_DEBUG("Adjusting job proxy memory limit (JobProxyMemoryReserve: %v, UserJobMemoryReserve: %v)",
            JobProxyMemoryReserve_,
            userJobSpec.memory_reserve());
    } else {
        JobProxyMemoryReserve_ = totalMemoryReserve;
    }

    std::vector<TString> annotations{
        Format("Type: SchedulerJob"),
        Format("OperationId: %v", OperationId_),
        Format("JobId: %v", JobId_),
        Format("JobType: %v", GetJobSpecHelper()->GetJobType()),
        Format("AuthenticatedUser: %v", authenticatedUser),
    };

    for (auto* descriptor : {
        &GetJobSpecHelper()->GetJobIOConfig()->TableReader->WorkloadDescriptor,
        &GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
        &GetJobSpecHelper()->GetJobIOConfig()->ErrorFileWriter->WorkloadDescriptor
    })
    {
        descriptor->Annotations.insert(
            descriptor->Annotations.end(),
            annotations.begin(),
            annotations.end());
    }

    {
        const auto& blockCacheConfig = GetJobSpecHelper()->GetJobIOConfig()->BlockCache;
        blockCacheConfig->CompressedData->ShardCount = 1;
        blockCacheConfig->UncompressedData->ShardCount = 1;

        ReaderBlockCache_ = CreateClientBlockCache(
            GetJobSpecHelper()->GetJobIOConfig()->BlockCache,
            EBlockType::CompressedData | EBlockType::UncompressedData,
            GetNullMemoryUsageTracker());
    }
}

void TJobProxy::Run()
{
    auto startTime = Now();
    auto resultOrError = BIND(&TJobProxy::DoRun, Unretained(this))
        .AsyncVia(JobThread_->GetInvoker())
        .Run()
        .Get();
    auto finishTime = Now();

    TJobResult result;
    if (!resultOrError.IsOK()) {
        YT_LOG_ERROR(resultOrError, "Job failed");
        ToProto(result.mutable_error(), resultOrError);
    } else {
        result = resultOrError.Value();
    }

    // Reliably terminate all async calls before reporting result.
    if (HeartbeatExecutor_) {
        WaitFor(HeartbeatExecutor_->Stop())
            .ThrowOnError();
    }

    if (MemoryWatchdogExecutor_) {
        WaitFor(MemoryWatchdogExecutor_->Stop())
            .ThrowOnError();
    }

    if (CpuMonitor_) {
        WaitFor(CpuMonitor_->Stop())
            .ThrowOnError();
    }

    RpcServer_->Stop()
        .WithTimeout(RpcServerShutdownTimeout)
        .Get();

    if (auto job = FindJob()) {
        auto failedChunkIds = job->GetFailedChunkIds();
        if (!failedChunkIds.empty()) {
            YT_LOG_INFO("Failed chunks found (ChunkIds: %v)",
                failedChunkIds);
        }

        // For erasure chunks, replace part id with whole chunk id.
        auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
        for (auto chunkId : failedChunkIds) {
            auto actualChunkId = IsErasureChunkPartId(chunkId)
                ? ErasureChunkIdFromPartId(chunkId)
                : chunkId;
            ToProto(schedulerResultExt->add_failed_chunk_ids(), actualChunkId);
        }

        auto interruptDescriptor = job->GetInterruptDescriptor();

        if (!interruptDescriptor.UnreadDataSliceDescriptors.empty()) {
            auto inputStatistics = GetTotalInputDataStatistics(job->GetStatistics());
            if (inputStatistics.row_count() > 0) {
                // NB(psushin): although we definitely have read some of the rows, the job may have made no progress,
                // since all of these row are from foreign tables, and therefor the ReadDataSliceDescriptors is empty.
                // Still we would like to treat such a job as interrupted, otherwise it may lead to an infinite sequence
                // of jobs being aborted by splitter instead of interrupts.

                ToProto(
                    schedulerResultExt->mutable_unread_chunk_specs(),
                    schedulerResultExt->mutable_chunk_spec_count_per_unread_data_slice(),
                    schedulerResultExt->mutable_virtual_row_index_per_unread_data_slice(),
                    interruptDescriptor.UnreadDataSliceDescriptors);
                ToProto(
                    schedulerResultExt->mutable_read_chunk_specs(),
                    schedulerResultExt->mutable_chunk_spec_count_per_read_data_slice(),
                    schedulerResultExt->mutable_virtual_row_index_per_read_data_slice(),
                    interruptDescriptor.ReadDataSliceDescriptors);

                schedulerResultExt->set_restart_needed(true);

                YT_LOG_DEBUG(
                    "Interrupt descriptor found (UnreadDescriptorCount: %v, ReadDescriptorCount: %v, SchedulerResultExt: %v)",
                    interruptDescriptor.UnreadDataSliceDescriptors.size(),
                    interruptDescriptor.ReadDataSliceDescriptors.size(),
                    schedulerResultExt->ShortDebugString());
            } else {
                if (result.error().code() == 0) {
                    ToProto(
                        result.mutable_error(),
                        TError(EErrorCode::JobNotPrepared, "Job did not read anything"));
                }
            }
        }


        auto schedulerJobSpecExt = GetJobSpecHelper()->GetSchedulerJobSpecExt();
        if (schedulerJobSpecExt.has_user_job_spec()) {
            const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();
            if (userJobSpec.has_restart_exit_code()) {
                auto error = FromProto<TError>(result.error());
                auto userJobFailedError = error.FindMatching(EErrorCode::UserJobFailed);
                if (userJobFailedError) {
                    auto processFailedError = userJobFailedError->FindMatching(EProcessErrorCode::NonZeroExitCode);
                    if (processFailedError && processFailedError->Attributes().Get<int>("exit_code", 0) == userJobSpec.restart_exit_code()) {
                        YT_LOG_DEBUG("Job exited with code that indicates job restart (ExitCode: %v)",
                            userJobSpec.restart_exit_code());
                        schedulerResultExt->set_restart_needed(true);
                        ToProto(result.mutable_error(), TError());
                    }
                }
            }
        }
    }

    auto statistics = ConvertToYsonString(GetStatistics());

    EnsureStderrResult(&result);

    ReportResult(result, statistics, startTime, finishTime);
}

IJobPtr TJobProxy::CreateBuiltinJob()
{
    auto jobType = GetJobSpecHelper()->GetJobType();
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
            YT_ABORT();
    }
}

TString TJobProxy::AdjustPath(const TString& path) const
{
    YT_VERIFY(path.StartsWith(GetPreparationPath()));
    auto pathSuffix = path.substr(GetPreparationPath().size() + 1);
    auto adjustedPath = NFS::CombinePaths(GetSlotPath(), pathSuffix);
    return adjustedPath;
}

void TJobProxy::SetJob(IJobPtr job)
{
    Job_.Store(std::move(job));
}

IJobPtr TJobProxy::FindJob() const
{
    return Job_.Load();
}

IJobPtr TJobProxy::GetJobOrThrow()
{
    auto job = FindJob();
    if (!job) {
        THROW_ERROR_EXCEPTION("Job is not initialized yet");
    }
    return job;
}

void TJobProxy::SetJobProxyEnvironment(IJobProxyEnvironmentPtr environment)
{
    JobProxyEnvironment_.Store(std::move(environment));
}

IJobProxyEnvironmentPtr TJobProxy::FindJobProxyEnvironment() const
{
    return JobProxyEnvironment_.Load();
}

TJobResult TJobProxy::DoRun()
{
    IJobPtr job;

    try {
        auto environment = CreateJobProxyEnvironment(Config_->JobEnvironment);
        SetJobProxyEnvironment(environment);

        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(Config_->Addresses, Config_->Rack, Config_->DataCenter);

        TrafficMeter_ = New<TTrafficMeter>(LocalDescriptor_.GetDataCenter());
        TrafficMeter_->Start();

        RpcServer_ = NRpc::NBus::CreateBusServer(CreateTcpBusServer(Config_->BusServer));
        RpcServer_->RegisterService(CreateJobProberService(this, GetControlInvoker()));
        RpcServer_->Start();

        auto supervisorClient = CreateTcpBusClient(Config_->SupervisorConnection);
        auto supervisorChannel = NRpc::NBus::CreateBusChannel(supervisorClient);

        SupervisorProxy_ = std::make_unique<TSupervisorServiceProxy>(supervisorChannel);
        SupervisorProxy_->SetDefaultTimeout(Config_->SupervisorRpcTimeout);

        RetrieveJobSpec();

        auto clusterConnection = NApi::NNative::CreateConnection(Config_->ClusterConnection);

        Client_ = clusterConnection->CreateNativeClient(TClientOptions::FromUser(GetJobUserName()));

        auto cpuMonitorConfig = ConvertTo<TJobCpuMonitorConfigPtr>(TYsonString(JobSpecHelper_->GetSchedulerJobSpecExt().job_cpu_monitor_config()));
        CpuMonitor_ = New<TCpuMonitor>(std::move(cpuMonitorConfig), JobThread_->GetInvoker(), this, CpuGuarantee_);

        if (Config_->JobThrottler) {
            YT_LOG_DEBUG("Job throttling enabled");

            InBandwidthThrottler_ = CreateInJobBandwidthThrottler(
                Config_->JobThrottler,
                supervisorChannel,
                GetJobSpecHelper()->GetJobIOConfig()->TableReader->WorkloadDescriptor,
                JobId_);

            OutBandwidthThrottler_ = CreateOutJobBandwidthThrottler(
                Config_->JobThrottler,
                supervisorChannel,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_);

            OutRpsThrottler_ = CreateOutJobRpsThrottler(
                Config_->JobThrottler,
                supervisorChannel,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_);
        } else {
            YT_LOG_DEBUG("Job throttling disabled");

            InBandwidthThrottler_ = GetUnlimitedThrottler();
            OutBandwidthThrottler_ = GetUnlimitedThrottler();
            OutRpsThrottler_ = GetUnlimitedThrottler();
        }

        const auto& schedulerJobSpecExt = GetJobSpecHelper()->GetSchedulerJobSpecExt();

        if (schedulerJobSpecExt.has_yt_alloc_min_large_unreclaimable_bytes()) {
            NYTAlloc::SetMinLargeUnreclaimableBytes(schedulerJobSpecExt.yt_alloc_min_large_unreclaimable_bytes());
        }
        if (schedulerJobSpecExt.has_yt_alloc_max_large_unreclaimable_bytes()) {
            NYTAlloc::SetMaxLargeUnreclaimableBytes(schedulerJobSpecExt.yt_alloc_max_large_unreclaimable_bytes());
        }
        JobProxyMemoryOvercommitLimit_ = schedulerJobSpecExt.has_job_proxy_memory_overcommit_limit()
            ? std::make_optional(schedulerJobSpecExt.job_proxy_memory_overcommit_limit())
            : std::nullopt;

        RefCountedTrackerLogPeriod_ = FromProto<TDuration>(schedulerJobSpecExt.job_proxy_ref_counted_tracker_log_period());

        if (environment) {
            environment->SetCpuGuarantee(CpuGuarantee_);
            if (schedulerJobSpecExt.has_user_job_spec() &&
                schedulerJobSpecExt.user_job_spec().set_container_cpu_limit())
            {
                auto limit = schedulerJobSpecExt.user_job_spec().container_cpu_limit() > 0
                    ? schedulerJobSpecExt.user_job_spec().container_cpu_limit()
                    : CpuGuarantee_.load();
                environment->SetCpuLimit(limit);
            }
        }

        InputNodeDirectory_ = New<NNodeTrackerClient::TNodeDirectory>();
        InputNodeDirectory_->MergeFrom(schedulerJobSpecExt.input_node_directory());

        HeartbeatExecutor_ = New<TPeriodicExecutor>(
            JobThread_->GetInvoker(),
            BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)),
            Config_->HeartbeatPeriod);

        auto jobEnvironmentConfig = ConvertTo<TJobEnvironmentConfigPtr>(Config_->JobEnvironment);
        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            JobThread_->GetInvoker(),
            BIND(&TJobProxy::CheckMemoryUsage, MakeWeak(this)),
            jobEnvironmentConfig->MemoryWatchdogPeriod);

        if (schedulerJobSpecExt.has_user_job_spec()) {
            job = CreateUserJob(
                this,
                schedulerJobSpecExt.user_job_spec(),
                JobId_,
                Ports_,
                std::make_unique<TUserJobWriteController>(this));
        } else {
            job = CreateBuiltinJob();
        }

        SetJob(job);
        job->Initialize();

        OnSpawned();
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to prepare job proxy");
        Exit(EJobProxyExitCode::JobProxyPrepareFailed);
    }

    job->PrepareArtifacts();
    OnArtifactsPrepared();

    MemoryWatchdogExecutor_->Start();
    HeartbeatExecutor_->Start();
    CpuMonitor_->Start();

    return job->Run();
}

void TJobProxy::ReportResult(
    const TJobResult& result,
    const TYsonString& statistics,
    TInstant startTime,
    TInstant finishTime)
{
    if (!SupervisorProxy_) {
        YT_LOG_ERROR("Supervisor channel is not available");
        Exit(EJobProxyExitCode::ResultReportFailed);
    }

    auto req = SupervisorProxy_->OnJobFinished();
    ToProto(req->mutable_job_id(), JobId_);
    *req->mutable_result() = result;
    req->set_statistics(statistics.ToString());
    req->set_start_time(ToProto<i64>(startTime));
    req->set_finish_time(ToProto<i64>(finishTime));
    auto job = FindJob();
    if (job && GetJobSpecHelper()->GetSchedulerJobSpecExt().has_user_job_spec()) {
        ToProto(req->mutable_core_infos(), job->GetCoreInfos());

        try {
            auto failContext = job->GetFailContext();
            if (failContext) {
                req->set_fail_context(*failContext);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job fail context on teardown");
        }

        try {
            req->set_job_stderr(GetStderr());
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job stderr on teardown");
        }

        try{
            auto profile = job->GetProfile();
            if (profile) {
                req->set_profile_type(profile->Type);
                req->set_profile_blob(profile->Blob);
                req->set_profiling_probability(profile->ProfilingProbability);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job profile on teardown");
        }
    }

    auto rspOrError = req->Invoke().Get();
    if (!rspOrError.IsOK()) {
        YT_LOG_ERROR(rspOrError, "Failed to report job result");
        Exit(EJobProxyExitCode::ResultReportFailed);
    }
}

TStatistics TJobProxy::GetStatistics() const
{
    TStatistics statistics;

    if (auto job = FindJob()) {
        statistics = job->GetStatistics();
    }

    if (auto environment = FindJobProxyEnvironment()) {
        try {
            auto cpuStatistics = environment->GetCpuStatistics();
            statistics.AddSample("/job_proxy/cpu", cpuStatistics);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unable to get CPU statistics from resource controller");
        }

        try {
            auto blockIOStatistics = environment->GetBlockIOStatistics();
            statistics.AddSample("/job_proxy/block_io", blockIOStatistics);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unable to get block IO statistics from resource controller");
        }
    }

    if (JobProxyMaxMemoryUsage_ > 0) {
        statistics.AddSample("/job_proxy/max_memory", JobProxyMaxMemoryUsage_);
    }

    if (JobProxyMemoryReserve_ > 0) {
        statistics.AddSample("/job_proxy/memory_reserve", JobProxyMemoryReserve_);
    }

    FillTrafficStatistics(JobProxyTrafficStatisticsPrefix, statistics, TrafficMeter_);

    CpuMonitor_->FillStatistics(statistics);

    statistics.SetTimestamp(TInstant::Now());

    return statistics;
}

IUserJobEnvironmentPtr TJobProxy::CreateUserJobEnvironment(const TJobSpecEnvironmentOptions& options) const
{
    auto environment = FindJobProxyEnvironment();
    YT_VERIFY(environment);

    auto createRootFS = [&] () -> std::optional<TRootFS> {
        if (!Config_->RootPath) {
            YT_LOG_DEBUG("Job is not using custom root fs");
            return std::nullopt;
        }

        if (Config_->TestRootFS) {
            YT_LOG_DEBUG("Job is running in testing root fs mode");
            return std::nullopt;
        }

        YT_LOG_DEBUG("Job is using custom root fs (Path: %v)", Config_->RootPath);

        TRootFS rootFS {
            .IsRootReadOnly = !Config_->MakeRootFSWritable,
            .RootPath = *Config_->RootPath
        };

        for (const auto& bind : Config_->Binds) {
            rootFS.Binds.emplace_back(TBind{bind->ExternalPath, bind->InternalPath, bind->ReadOnly});
        }

        rootFS.Binds.emplace_back(TBind{GetPreparationPath(), GetSlotPath(), /*readOnly*/ false});
        for (const auto& tmpfsPath : Config_->TmpfsManager->TmpfsPaths) {
            rootFS.Binds.emplace_back(TBind{tmpfsPath, AdjustPath(tmpfsPath), /*readOnly*/ false});
        }

        // Temporary workaround for nirvana - make tmp directories writable.
        auto tmpPath = NFS::CombinePaths(NFs::CurrentWorkingDirectory(), SandboxDirectoryNames[ESandboxKind::Tmp]);
        rootFS.Binds.emplace_back(TBind{tmpPath, "/tmp", /*readOnly*/ false});
        rootFS.Binds.emplace_back(TBind{tmpPath, "/var/tmp", /*readOnly*/ false});

        return rootFS;
    };

    TUserJobEnvironmentOptions environmentOptions {
        .RootFS = createRootFS(),
        .GpuDevices = Config_->GpuDevices,
        .HostName = Config_->HostName,
        .NetworkAddresses = Config_->NetworkAddresses,
        .EnablePortoMemoryTracking = options.EnablePortoMemoryTracking,
        .EnableCudaGpuCoreDump = options.EnableGpuCoreDumps,
        .EnablePorto = options.EnablePorto,
        .ThreadLimit = options.ThreadLimit
    };

    if (options.EnableCoreDumps) {
        environmentOptions.SlotCoreWatcherDirectory = NFS::CombinePaths({GetSlotPath(), "cores"});
        environmentOptions.CoreWatcherDirectory = NFS::CombinePaths({GetPreparationPath(), "cores"});
    }

    return environment->CreateUserJobEnvironment(
        JobId_,
        environmentOptions);
}

TJobProxyConfigPtr TJobProxy::GetConfig() const
{
    return Config_;
}

TOperationId TJobProxy::GetOperationId() const
{
    return OperationId_;
}

TJobId TJobProxy::GetJobId() const
{
    return JobId_;
}

TString TJobProxy::GetJobUserName() const
{
    return Format("%v:%v",
        NSecurityClient::JobUserName,
        JobSpecHelper_->GetSchedulerJobSpecExt().authenticated_user());
}

const IJobSpecHelperPtr& TJobProxy::GetJobSpecHelper() const
{
    YT_VERIFY(JobSpecHelper_);
    return JobSpecHelper_;
}

void TJobProxy::UpdateResourceUsage()
{
    // Fire-and-forget.
    auto req = SupervisorProxy_->UpdateResourceUsage();
    ToProto(req->mutable_job_id(), JobId_);
    auto* resourceUsage = req->mutable_resource_usage();
    resourceUsage->set_cpu(CpuGuarantee_);
    resourceUsage->set_network(NetworkUsage_);
    resourceUsage->set_memory(RequestedMemoryReserve_);
    req->Invoke().Subscribe(BIND(&TJobProxy::OnResourcesUpdated, MakeWeak(this), RequestedMemoryReserve_.load()));
}

void TJobProxy::OnSpawned()
{
    auto req = SupervisorProxy_->OnJobProxySpawned();
    ToProto(req->mutable_job_id(), JobId_);
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TJobProxy::OnArtifactsPrepared()
{
    auto req = SupervisorProxy_->OnArtifactsPrepared();
    ToProto(req->mutable_job_id(), JobId_);
    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TJobProxy::SetUserJobMemoryUsage(i64 memoryUsage)
{
    UserJobCurrentMemoryUsage_ = memoryUsage;
}

void TJobProxy::OnResourcesUpdated(i64 memoryReserve, const TError& error)
{
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Failed to update resource usage");
        if (auto job = FindJob()) {
            job->Cleanup();
        }
        Exit(EJobProxyExitCode::ResourcesUpdateFailed);
    }

    if (ApprovedMemoryReserve_ < memoryReserve) {
        YT_LOG_DEBUG("Successfully updated resource usage (MemoryReserve: %v)", memoryReserve);
        ApprovedMemoryReserve_ = memoryReserve;
    }
}

void TJobProxy::ReleaseNetwork()
{
    YT_LOG_DEBUG("Releasing network");
    NetworkUsage_ = 0;
    UpdateResourceUsage();
}

void TJobProxy::OnPrepared()
{
    YT_LOG_DEBUG("Job prepared");

    auto req = SupervisorProxy_->OnJobPrepared();
    ToProto(req->mutable_job_id(), JobId_);
    req->Invoke();
}

void TJobProxy::PrepareArtifact(
    const TString& artifactName,
    const TString& pipePath)
{
    YT_LOG_DEBUG("Asking node to prepare artifact (ArtifactName: %v, PipePath: %v)",
        artifactName,
        pipePath);

    auto req = SupervisorProxy_->PrepareArtifact();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_artifact_name(artifactName);
    req->set_pipe_path(pipePath);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

void TJobProxy::OnArtifactPreparationFailed(
    const TString& artifactName,
    const TString& artifactPath,
    const TError& error)
{
    YT_LOG_DEBUG(error, "Artifact preparation failed (ArtifactName: %v, ArtifactPath: %v)",
        artifactName,
        artifactPath);

    auto req = SupervisorProxy_->OnArtifactPreparationFailed();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_artifact_name(artifactName);
    req->set_artifact_path(artifactPath);
    ToProto(req->mutable_error(), error);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

NApi::NNative::IClientPtr TJobProxy::GetClient() const
{
    return Client_;
}

IBlockCachePtr TJobProxy::GetReaderBlockCache() const
{
    return ReaderBlockCache_;
}

IBlockCachePtr TJobProxy::GetWriterBlockCache() const
{
    return GetNullBlockCache();
}

TNodeDirectoryPtr TJobProxy::GetInputNodeDirectory() const
{
    return InputNodeDirectory_;
}

const NNodeTrackerClient::TNodeDescriptor& TJobProxy::LocalDescriptor() const
{
    return LocalDescriptor_;
}

void TJobProxy::CheckMemoryUsage()
{
    i64 jobProxyMemoryUsage = 0;
    try {
        jobProxyMemoryUsage = GetProcessMemoryUsage().Rss;
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get process memory usage");
        return;
    }

    JobProxyMaxMemoryUsage_ = std::max(JobProxyMaxMemoryUsage_.load(), jobProxyMemoryUsage);

    YT_LOG_DEBUG("Job proxy memory check (JobProxyMemoryUsage: %v, JobProxyMaxMemoryUsage: %v, JobProxyMemoryReserve: %v, UserJobCurrentMemoryUsage: %v)",
        jobProxyMemoryUsage,
        JobProxyMaxMemoryUsage_.load(),
        JobProxyMemoryReserve_,
        UserJobCurrentMemoryUsage_.load());

    YT_LOG_DEBUG("YTAlloc counters (%v)", NYTAlloc::FormatAllocationCounters());

    constexpr double JobProxyMaxMemoryUsageLoggingExponentialFactor = 1.2;

    auto usage = JobProxyMaxMemoryUsage_.load();
    if (usage > JobProxyMemoryReserve_ &&
        usage > LastLoggedJobProxyMaxMemoryUsage_ * JobProxyMaxMemoryUsageLoggingExponentialFactor)
    {
        if (TInstant::Now() - LastRefCountedTrackerLogTime_ > RefCountedTrackerLogPeriod_) {
            YT_LOG_WARNING("Job proxy used more memory than estimated "
                "(JobProxyMaxMemoryUsage: %v, JobProxyMemoryReserve: %v, RefCountedTracker: %v)",
                usage,
                JobProxyMemoryReserve_,
                TRefCountedTracker::Get()->GetDebugInfo(2 /* sortByColumn */));
            LastRefCountedTrackerLogTime_ = TInstant::Now();
            LastLoggedJobProxyMaxMemoryUsage_ = usage;
        }
    }

    if (JobProxyMemoryOvercommitLimit_ && jobProxyMemoryUsage > JobProxyMemoryReserve_ + *JobProxyMemoryOvercommitLimit_) {
        YT_LOG_FATAL("Job proxy exceeded the memory overcommit limit "
            "(JobProxyMemoryUsage: %v, JobProxyMemoryReserve: %v, MemoryOvercommitLimit: %v, RefCountedTracker: %v)",
            jobProxyMemoryUsage,
            JobProxyMemoryReserve_,
            JobProxyMemoryOvercommitLimit_,
            TRefCountedTracker::Get()->GetDebugInfo(2 /* sortByColumn */));
    }

    i64 totalMemoryUsage = UserJobCurrentMemoryUsage_ + jobProxyMemoryUsage;

    if (TotalMaxMemoryUsage_ < totalMemoryUsage) {
        YT_LOG_DEBUG("Total memory usage increased (OldTotalMaxMemoryUsage: %v, NewTotalMaxMemoryUsage: %v)",
            TotalMaxMemoryUsage_,
            totalMemoryUsage);
        TotalMaxMemoryUsage_ = totalMemoryUsage;
        if (TotalMaxMemoryUsage_ > ApprovedMemoryReserve_) {
            YT_LOG_ERROR("Total memory usage exceeded the limit approved by the node "
                "(TotalMaxMemoryUsage: %v, ApprovedMemoryReserve: %v, AheadMemoryReserve: %v)",
                TotalMaxMemoryUsage_,
                ApprovedMemoryReserve_.load(),
                Config_->AheadMemoryReserve);
            // TODO(psushin): first improve memory estimates with data weights.
            // Exit(EJobProxyExitCode::ResourceOverdraft);
        }
    }
    i64 memoryReserve = TotalMaxMemoryUsage_ + Config_->AheadMemoryReserve;
    if (RequestedMemoryReserve_ < memoryReserve) {
        YT_LOG_DEBUG("Request node for memory usage increase (OldMemoryReserve: %v, NewMemoryReserve: %v)",
            RequestedMemoryReserve_.load(),
            memoryReserve);
        RequestedMemoryReserve_ = memoryReserve;
        UpdateResourceUsage();
    }
}

void TJobProxy::EnsureStderrResult(TJobResult* jobResult)
{
    const auto& schedulerJobSpecExt = GetJobSpecHelper()->GetSchedulerJobSpecExt();
    const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();

    auto* schedulerJobResultExt = jobResult->MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);

    // If we were provided with stderr_table_spec we are expected to write stderr and provide some results.
    if (userJobSpec.has_stderr_table_spec() && !schedulerJobResultExt->has_stderr_table_boundary_keys()) {
        // If error occurred during user job initialization, stderr blob table writer may not have been created at all.
        YT_LOG_WARNING("Stderr table boundary keys are absent");
        auto* stderrBoundaryKeys = schedulerJobResultExt->mutable_stderr_table_boundary_keys();
        stderrBoundaryKeys->set_sorted(true);
        stderrBoundaryKeys->set_unique_keys(true);
    }
}

void TJobProxy::Exit(EJobProxyExitCode exitCode)
{
    if (auto job = FindJob()) {
        job->Cleanup();
    }

    NLogging::TLogManager::Get()->Shutdown();
    _exit(static_cast<int>(exitCode));
}

bool TJobProxy::TrySetCpuGuarantee(double cpuGuarantee)
{
    if (auto environment = FindJobProxyEnvironment()) {
        try {
            environment->SetCpuGuarantee(cpuGuarantee);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Failed to set CPU share (OldCpuShare: %v, NewCpuShare: %v)",
                CpuGuarantee_.load(),
                cpuGuarantee);
            return false;
        }
        YT_LOG_INFO("Changed CPU share (OldCpuShare: %v, NewCpuShare: %v)",
            CpuGuarantee_.load(),
            cpuGuarantee);
        CpuGuarantee_ = cpuGuarantee;
        UpdateResourceUsage();
        return true;
    } else {
        YT_LOG_INFO("Unable to change CPU share: environment is not set");
        return false;
    }
}

TDuration TJobProxy::GetSpentCpuTime() const
{
    auto result = TDuration::Zero();

    if (auto job = FindJob()) {
        auto jobCpu = job->GetCpuStatistics();
        result += jobCpu.SystemTime + jobCpu.UserTime;
    }

    if (auto environment = FindJobProxyEnvironment()) {
        auto proxyCpu = environment->GetCpuStatistics();
        result += proxyCpu.SystemTime + proxyCpu.UserTime;
    }

    return result;
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

} // namespace NYT::NJobProxy
