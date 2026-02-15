#include "job_proxy.h"

#include "cpu_monitor.h"
#include "job_prober_service.h"
#include "job_throttler.h"
#include "merge_job.h"
#include "partition_job.h"
#include "partition_sort_job.h"
#include "remote_copy_job.h"
#include "shallow_merge_job.h"
#include "signature_proxy.h"
#include "simple_sort_job.h"
#include "job_api_service.h"
#include "sorted_merge_job.h"
#include "user_job.h"
#include "user_job_write_controller.h"

#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>
#include <yt/yt/server/lib/controller_agent/statistics.h>

#include <yt/yt/server/lib/job_proxy/events_on_fs.h>

#include <yt/yt/server/lib/exec_node/proto/supervisor_service.pb.h>

#include <yt/yt/server/lib/rpc_proxy/access_checker.h>
#include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>

#include <yt/yt/server/lib/shuffle_server/shuffle_service.h>

#include <yt/yt/server/lib/user_job/config.h>

#include <yt/yt/server/tools/proc.h>
#include <yt/yt/server/tools/tools.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/auth/native_authentication_manager.h>
#include <yt/yt/ytlib/auth/tvm_bridge.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>
#include <yt/yt/ytlib/chunk_client/traffic_meter.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/config.h>
#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/library/containers/porto_helpers.h>

#include <yt/yt/library/orchid/orchid_service.h>

#include <yt/yt/library/auth_server/authentication_manager.h>
#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/signature/validator.h>

#include <yt/yt/client/table_client/column_rename_descriptor.h>
#include <yt/yt/client/table_client/config.h>

#include <yt/yt/library/auth/credentials_injecting_channel.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/program.h>

#include <yt/yt/library/dns_over_rpc/client/dns_over_rpc_resolver.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/grpc/server.h>

#include <yt/yt/core/rpc/http/server.h>

#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/net/listener.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <tcmalloc/malloc_extension.h>

#include <util/system/fs.h>

#include <sys/resource.h>

namespace NYT::NJobProxy {

using namespace NRpcProxy;

using namespace NApi;
using namespace NBus;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NContainers;
using namespace NControllerAgent::NProto;
using namespace NControllerAgent;
using namespace NExecNode::NProto;
using namespace NExecNode;
using namespace NJobProberClient;
using namespace NJobProxy;
using namespace NLogging;
using namespace NNet;
using namespace NProfiling;
using namespace NRpc;
using namespace NScheduler::NProto;
using namespace NScheduler;
using namespace NScheduler;
using namespace NServer;
using namespace NShuffleServer;
using namespace NSignature;
using namespace NStatisticPath;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NUserJob;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

static const TString ExecutorConfigFileName = "executor_config.yson";

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

void FillStatistics(auto& req, const IJobPtr& job, const TStatistics& enrichedStatistics)
{
    auto extendedStatistics = job->GetStatistics();
    req->set_statistics(ToProto(ConvertToYsonString(enrichedStatistics)));
    *req->mutable_total_input_data_statistics() = std::move(extendedStatistics.TotalInputStatistics.DataStatistics);
    for (auto& statistics : extendedStatistics.OutputStatistics) {
        *req->add_output_data_statistics() = std::move(statistics.DataStatistics);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TJobProxy::TJobProxy(
    TJobProxyInternalConfigPtr config,
    TOperationId operationId,
    TJobId jobId)
    : Config_(std::move(config))
    , OperationId_(operationId)
    , JobId_(jobId)
    , JobThread_(New<TActionQueue>("JobMain"))
    , ControlThread_(New<TActionQueue>("Control"))
    , Logger(JobProxyLogger().WithTag("OperationId: %v, JobId: %v",
        OperationId_,
        JobId_))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger(), Config_);
    } else {
        WarnForUnrecognizedOptions(Logger(), Config_);
    }
}

TString TJobProxy::GetPreparationPath() const
{
    return NFs::CurrentWorkingDirectory();
}

TString TJobProxy::GetSlotPath() const
{
    if ((!Config_->RootPath && !Config_->DockerImage) || Config_->TestRootFS) {
        return NFs::CurrentWorkingDirectory();
    }

    return "/slot";
}

TString TJobProxy::GetJobProxyUnixDomainSocketPath() const
{
    // TODO(babenko): migrate to std::string
    return AdjustPath(TString(*Config_->BusServer->UnixDomainSocketPath));
}

std::string TJobProxy::GetJobProxyGrpcUnixDomainSocketPath() const
{
    constexpr std::string_view prefix = "unix:";

    auto addresses = Config_->GrpcServer->Addresses;
    YT_VERIFY(addresses.size() == 1);
    YT_VERIFY(addresses[0]->Address.starts_with(prefix));

    return AdjustPath(TString(addresses[0]->Address.substr(prefix.size())));
}

std::string TJobProxy::GetJobProxyHttpUnixDomainSocketPath() const
{
    return AdjustPath(TString(Config_->HttpServerUdsPath));
}

std::vector<NChunkClient::TChunkId> TJobProxy::DumpInputContext(TTransactionId transactionId)
{
    auto job = GetJobOrThrow();
    return job->DumpInputContext(transactionId);
}

TGetJobStderrResponse TJobProxy::GetStderr(const TGetJobStderrOptions& options)
{
    auto job = GetJobOrThrow();
    return job->GetStderr(options);
}

TPollJobShellResponse TJobProxy::PollJobShell(
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

void TJobProxy::GracefulAbort(TError error)
{
    auto job = GetJobOrThrow();
    job->GracefulAbort(std::move(error));
}

void TJobProxy::Fail(TError error)
{
    auto job = GetJobOrThrow();
    job->Fail(std::move(error));
}

TSharedRef TJobProxy::DumpSensors()
{
    auto tags = TTagSet();
    const auto& host = Config_->SolomonExporter->Host;

    if (host) {
        tags = tags
            .WithTag(NProfiling::TTag{"host", ToString(*host)})
            .WithTag(NProfiling::TTag{"slot_index", ToString(Config_->SlotIndex)}, /*parent*/ -1);
    }

    const auto& jobSpecExt = GetJobSpecHelper()->GetJobSpecExt();
    if (jobSpecExt.has_user_job_spec() && jobSpecExt.user_job_spec().has_monitoring_config()) {
        std::string jobProxyDescriptor = jobSpecExt.user_job_spec().monitoring_config().job_descriptor();
        if (host) {
            tags = tags
                // Alternative to host tag.
                .WithAlternativeTag(NProfiling::TTag{"job_descriptor", jobProxyDescriptor}, /*alternativeTo*/ -2)
                .WithExtensionTag(NProfiling::TTag{"host", ""}, /*extensionTo*/ -1);
        } else {
            tags = tags
                .WithRequiredTag(NProfiling::TTag{"job_descriptor", jobProxyDescriptor})
                .WithExtensionTag(NProfiling::TTag{"host", ""}, /*extensionTo*/ -1);
        }
    }

    for (const auto& [key, value] : Config_->SolomonExporter->InstanceTags) {
        tags = tags
            .WithRequiredTag(NProfiling::TTag{key, value});
    }

    return SolomonExporter_->DumpSensors(/*customTagSet*/ tags);
}

IServerPtr TJobProxy::GetRpcServer() const
{
    return RpcServer_;
}

TTrafficMeterPtr TJobProxy::GetTrafficMeter() const
{
    return TrafficMeter_;
}

IThroughputThrottlerPtr TJobProxy::GetInBandwidthThrottler(const TClusterName& clusterName) const
{
    auto guard = Guard(InBandwidthThrottlersSpinLock_);

    auto it = InBandwidthThrottlers_.find(clusterName);
    if (it == InBandwidthThrottlers_.end()) {
        NConcurrency::IThroughputThrottlerPtr throttler;
        if (Config_->JobThrottler) {
            // Throttling is enabled.
            throttler = CreateInJobBandwidthThrottler(
                Config_->JobThrottler,
                SupervisorChannel_,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_,
                clusterName,
                Logger);
        } else {
            // Throttling is disabled.
            throttler = GetUnlimitedThrottler();
        }
        it = InBandwidthThrottlers_.emplace(clusterName, std::move(throttler)).first;
    }

    return it->second;
}

IThroughputThrottlerPtr TJobProxy::GetOutBandwidthThrottler() const
{
    return OutBandwidthThrottler_;
}

IThroughputThrottlerPtr TJobProxy::GetOutRpsThrottler() const
{
    return OutRpsThrottler_;
}

IThroughputThrottlerPtr TJobProxy::GetUserJobContainerCreationThrottler() const
{
    return UserJobContainerCreationThrottler_;
}

void TJobProxy::SendHeartbeat()
{
    YT_ASSERT_THREAD_AFFINITY(JobThread);

    auto job = FindJob();
    if (!job) {
        return;
    }

    // Despite the fact that the heartbeat is sent from a single thread,
    // it is possible that messages are reordered while being sent over
    // the network for various reasons, e.g., retrying. To handle this,
    // use epoch numbers to discard outdated messages.
    i64 epoch = HeartbeatEpoch_++;
    YT_LOG_DEBUG("Reporting heartbeat to supervisor (HeartbeatEpoch: %v)", epoch);

    auto req = SupervisorProxy_->OnJobProgress();
    ToProto(req->mutable_job_id(), JobId_);
    req->set_progress(job->GetProgress());
    FillStatistics(req, job, GetEnrichedStatistics());
    req->set_stderr_size(job->GetStderrSize());
    req->set_has_job_trace(job->HasJobTrace());
    req->set_epoch(epoch);
    if (auto time = job->GetLastProgressSaveTime(); time.has_value()) {
        req->set_last_progress_save_time(ToProto(*time));
    }

    req->Invoke().Subscribe(BIND(&TJobProxy::OnHeartbeatResponse, MakeWeak(this)));
}

void TJobProxy::OnHeartbeatResponse(const TError& error)
{
    if (!error.IsOK()) {
        // NB: User process is not killed here.
        // Good user processes are supposed to die themselves
        // when io pipes are closed.
        // Bad processes will die at container shutdown.
        YT_LOG_ERROR(error, "Error sending heartbeat to supervisor");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }

    YT_LOG_DEBUG("Successfully reported heartbeat to supervisor");
}

void TJobProxy::LogJobSpec(TJobSpec jobSpec)
{
    // We patch copy of job spec for better reading experience.
    if (jobSpec.HasExtension(TJobSpecExt::job_spec_ext)) {
        auto* jobSpecExt = jobSpec.MutableExtension(TJobSpecExt::job_spec_ext);
        // Some fields are serialized in binary YSON, let's make them human readable.
        #define TRANSFORM_TO_PRETTY_YSON(object, field) \
            if (object->has_ ## field() && !object->field().empty()) { \
                object->set_ ## field(ConvertToYsonString(TYsonString(object->field()), EYsonFormat::Pretty).ToString()); \
            }
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, io_config);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, table_reader_options);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, acl);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, aco_name);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, job_cpu_monitor_config);
        TRANSFORM_TO_PRETTY_YSON(jobSpecExt, testing_options);

        // Input table specs may be large, so we print them separately.
        NProtoBuf::RepeatedPtrField<TTableInputSpec> primaryInputTableSpecs;
        jobSpecExt->mutable_input_table_specs()->Swap(&primaryInputTableSpecs);

        NProtoBuf::RepeatedPtrField<TTableInputSpec> foreignInputTableSpecs;
        jobSpecExt->mutable_foreign_input_table_specs()->Swap(&foreignInputTableSpecs);

        for (const auto& [name, inputTableSpecs] : {
            std::pair("primary", &primaryInputTableSpecs),
            std::pair("foreign", &foreignInputTableSpecs)
        })
        {
            TStringBuilder builder;
            for (const auto& tableSpec : *inputTableSpecs) {
                builder.AppendString(tableSpec.ShortDebugString());
                builder.AppendChar('\n');
            }
            YT_LOG_DEBUG("Job spec %v input table specs:\n%v",
                name,
                builder.Flush());
        }

        // Node directory is huge and useless when debugging.
        jobSpecExt->clear_input_node_directory();
    }

    NControllerAgent::SanitizeJobSpec(&jobSpec);

    YT_LOG_DEBUG("Job spec:\n%v", jobSpec.DebugString());
}

void TJobProxy::RetrieveJobSpec()
{
    YT_LOG_INFO("Requesting job spec");

    auto resourceUsage = [this] () {
        auto req = SupervisorProxy_->GetJobSpec();
        ToProto(req->mutable_job_id(), JobId_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_ERROR(rspOrError, "Failed to get job spec");
            Abort(EJobProxyExitCode::GetJobSpecFailed);
        }

        const auto& rsp = rspOrError.Value();

        if (rsp->job_spec().version() != GetJobSpecVersion()) {
            YT_LOG_WARNING("Invalid job spec version (Expected: %v, Actual: %v)",
                GetJobSpecVersion(),
                rsp->job_spec().version());
            Abort(EJobProxyExitCode::InvalidSpecVersion);
        }

        ChunkIdToOriginalSpec_ = PatchProxiedChunkSpecs(rsp->mutable_job_spec());
        JobSpecHelper_ = CreateJobSpecHelper(std::move(*rsp->mutable_job_spec()));

        Ports_ = FromProto<std::vector<int>>(rsp->ports());
        JobProxyRpcServerPort_ = YT_OPTIONAL_FROM_PROTO(*rsp, job_proxy_rpc_server_port);

        return std::move(*rsp->mutable_resource_usage());
    }();

    auto authenticatedUser = GetJobSpecHelper()->GetJobSpecExt().authenticated_user();
    YT_LOG_INFO(
        "Job spec received (JobType: %v, AuthenticatedUser: %v, ResourceLimits: {Cpu: %v, Memory: %v, Network: %v})",
        GetJobSpecHelper()->GetJobType(),
        authenticatedUser,
        resourceUsage.cpu(),
        resourceUsage.memory(),
        resourceUsage.network());

    // Job spec is copied intentionally.
    LogJobSpec(GetJobSpecHelper()->GetJobSpec());

    auto totalMemoryReserve = resourceUsage.memory();
    CpuGuarantee_ = resourceUsage.cpu();
    NetworkUsage_ = resourceUsage.network();

    // We never report to node less memory usage, than was initially reserved.
    TotalMaxMemoryUsage_ = totalMemoryReserve - Config_->AheadMemoryReserve;
    ApprovedMemoryReserve_ = totalMemoryReserve;
    RequestedMemoryReserve_ = totalMemoryReserve;

    const auto& jobSpecExt = JobSpecHelper_->GetJobSpecExt();
    if (jobSpecExt.has_user_job_spec()) {
        const auto& userJobSpec = jobSpecExt.user_job_spec();
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

void TJobProxy::DoRun()
{
    LastMemoryMeasureTime_ = Now();
    auto startTime = Now();
    auto resultOrError = WaitFor(BIND(&TJobProxy::RunJob, MakeStrong(this))
        .AsyncVia(JobThread_->GetInvoker())
        .Run());
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
        YT_LOG_INFO("Stopping heartbeat executor");
        WaitFor(HeartbeatExecutor_->Stop())
            .ThrowOnError();
        YT_LOG_INFO("Heartbeat executor stopped");
    }

    if (MemoryWatchdogExecutor_) {
        YT_LOG_INFO("Stopping memory watchdog executor");
        WaitFor(MemoryWatchdogExecutor_->Stop())
            .ThrowOnError();
        YT_LOG_INFO("Memory watchdog executor stopped");
    }

    if (CpuMonitor_) {
        YT_LOG_INFO("Stopping CPU monitor");
        WaitFor(CpuMonitor_->Stop())
            .ThrowOnError();
        YT_LOG_INFO("CPU monitor stopped");
    }

    if (GetJobSpecHelper()->HasSidecars()) {
        FindJobProxyEnvironment()->ShutdownSidecars();
    }

    {
        auto error = WaitFor(RpcServer_->Stop()
            .WithTimeout(RpcServerShutdownTimeout));
        YT_LOG_ERROR_UNLESS(error.IsOK(), error, "Error stopping RPC server");
    }


    if (GrpcServer_) {
        auto error = WaitFor(GrpcServer_->Stop()
            .WithTimeout(RpcServerShutdownTimeout));
        YT_LOG_ERROR_UNLESS(error.IsOK(), error, "Error stopping GRPC server");
    }

    if (HttpServer_) {
        auto error = WaitFor(HttpServer_->Stop()
            .WithTimeout(RpcServerShutdownTimeout));
        YT_LOG_ERROR_UNLESS(error.IsOK(), error, "Error stopping HTTP server");
    }

    FillJobResult(&result);
    FillStderrResult(&result);
    ReportResult(
        result,
        startTime,
        finishTime);

    SetJob(nullptr);

    if (auto tracer = GetGlobalTracer()) {
        tracer->Stop();
    }

    LogSystemStats();
}

void TJobProxy::Run()
{
    try {
        DoRun();
    } catch (const std::exception& ex) {
        if (Config_->AbortOnUncaughtException) {
            YT_LOG_FATAL(ex, "Abort on uncaught exception");
        } else {
            throw;
        }
    }
}

IJobPtr TJobProxy::CreateBuiltinJob()
{
    auto jobType = GetJobSpecHelper()->GetJobType();
    switch (jobType) {
        case EJobType::OrderedMerge:
            return CreateOrderedMergeJob(this);

        case EJobType::UnorderedMerge:
            return CreateUnorderedMergeJob(this);

        case EJobType::SortedMerge:
            return CreateSortedMergeJob(this);

        case EJobType::FinalSort:
        case EJobType::IntermediateSort:
            return CreatePartitionSortJob(this);

        case EJobType::SimpleSort:
            return CreateSimpleSortJob(this);

        case EJobType::Partition:
            return CreatePartitionJob(this);

        case EJobType::RemoteCopy:
            return CreateRemoteCopyJob(this);

        case EJobType::ShallowMerge:
            return CreateShallowMergeJob(this);

        default:
            YT_ABORT();
    }
}

TString TJobProxy::AdjustPath(const TString& path) const
{
    return NFS::CombinePaths(GetSlotPath(), NFS::GetRelativePath(GetPreparationPath(), path));
}

NYTree::IYPathServicePtr TJobProxy::CreateOrchidService()
{
    auto producer = BIND_NO_PROPAGATE([this, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
        auto info = GetJobOrThrow()->GetOrchidInfo();
        info.BuildOrchid(BuildYsonFluently(consumer));
    });

    return NYTree::IYPathService::FromProducer(std::move(producer));
}

void TJobProxy::InitializeOrchid()
{
    OrchidRoot_ = NYTree::CreateEphemeralNodeFactory(/*shouldHideAttributes*/true)->CreateMap();

    SetNodeByYPath(
        OrchidRoot_,
        "/job_proxy",
        CreateVirtualNode(CreateOrchidService()));
}

void TJobProxy::UpdateCumulativeMemoryUsage(i64 memoryUsage)
{
    auto now = Now();

    if (now <= LastMemoryMeasureTime_) {
        return;
    }

    CumulativeMemoryUsageMBSec_ += memoryUsage * (now - LastMemoryMeasureTime_).SecondsFloat() / 1_MB;
    LastMemoryMeasureTime_ = now;
}

void TJobProxy::SetJob(IJobPtr job)
{
    Job_.Store(std::move(job));
}

IJobPtr TJobProxy::FindJob() const
{
    return Job_.Acquire();
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

void TJobProxy::EnableRpcProxyInJobProxy(int rpcProxyWorkerThreadPoolSize, bool enableShuffleService)
{
    YT_VERIFY(Config_->OriginalClusterConnection);

    NApi::NNative::TConnectionOptions options;
    options.CreateQueueConsumerRegistrationManager = Config_->StartQueueConsumerRegistrationManager;

    auto connection = CreateNativeConnection(Config_->OriginalClusterConnection, std::move(options));
    connection->GetClusterDirectorySynchronizer()->Start();
    connection->GetNodeDirectorySynchronizer()->Start();
    if (Config_->StartQueueConsumerRegistrationManager) {
        connection->GetQueueConsumerRegistrationManagerOrThrow()->StartSync();
    }

    ApiServiceThreadPool_ = CreateThreadPool(rpcProxyWorkerThreadPoolSize, "RpcProxy");
    auto apiInvoker = ApiServiceThreadPool_->GetInvoker();

    auto signatureGenerator = New<TProxySignatureGenerator>(*SupervisorProxy_, JobId_);
    connection->SetSignatureGenerator(std::move(signatureGenerator));

    auto rootClient = connection->CreateNativeClient(NNative::TClientOptions::FromUser(NSecurityClient::RootUserName));

    if (enableShuffleService) {
        YT_VERIFY(!PublicRpcServer_);
        YT_VERIFY(!Config_->BusServer->Port.has_value());
        YT_VERIFY(JobProxyRpcServerPort_.has_value());
        Config_->BusServer->Port = *JobProxyRpcServerPort_;

        PublicRpcServer_ = NRpc::NBus::CreateBusServer(CreatePublicTcpBusServer(Config_->BusServer));
        PublicRpcServer_->Start();
        YT_LOG_INFO("Public RPC server started (JobProxyRpcServerPort: %v)", JobProxyRpcServerPort_);

        auto localServerAddress = BuildServiceAddress(NNet::GetLocalHostName(), *Config_->BusServer->Port);
        auto shuffleService = CreateShuffleService(
            apiInvoker,
            rootClient,
            localServerAddress);
        PublicRpcServer_->RegisterService(std::move(shuffleService));
        connection->RegisterShuffleService(localServerAddress);
        YT_LOG_INFO("Shuffle Service registered (LocalServerAddress: %v)", localServerAddress);
    }

    auto proxyCoordinator = CreateProxyCoordinator();
    proxyCoordinator->SetAvailableState(true);

    auto authenticationManager = NAuth::CreateAuthenticationManager(
        Config_->AuthenticationManager,
        NYT::NBus::TTcpDispatcher::Get()->GetXferPoller(),
        rootClient);

    auto signatureValidator = New<TProxySignatureValidator>(*SupervisorProxy_, JobId_);

    auto apiService = CreateApiService(
        Config_->JobProxyApiServiceStatic,
        apiInvoker,
        [=] (const std::string&, const TFairShareThreadPoolTag&) { return apiInvoker; },
        connection,
        authenticationManager->GetRpcAuthenticator(),
        proxyCoordinator,
        CreateNoopAccessChecker(),
        New<TSampler>(),
        RpcProxyLogger(),
        TProfiler(),
        std::move(signatureValidator));
    apiService->OnDynamicConfigChanged(Config_->JobProxyApiService);

    GetRpcServer()->RegisterService(std::move(apiService));
    YT_LOG_INFO("RPC proxy API service registered (ThreadCount: %v)", rpcProxyWorkerThreadPoolSize);
}

IJobProxyEnvironmentPtr TJobProxy::FindJobProxyEnvironment() const
{
    return JobProxyEnvironment_.Acquire();
}

TJobResult TJobProxy::RunJob()
{
    RootSpan_ = TTraceContext::NewRoot("Job");
    RootSpan_->SetRecorded();
    RootSpan_->AddTag("yt.job_id", ToString(JobId_));
    TTraceContextGuard guard(RootSpan_);

    IJobPtr job;
    IJobProxyEnvironmentPtr environment;

    try {
        if (Config_->TvmBridge && Config_->TvmBridgeConnection) {
            auto tvmBridgeClient = CreateBusClient(Config_->TvmBridgeConnection);
            auto tvmBridgeChannel = NRpc::NBus::CreateBusChannel(tvmBridgeClient);

            if (Config_->UseRetryingChannels) {
                tvmBridgeChannel = CreateRetryingChannel(
                    Config_->RetryingChannel,
                    std::move(tvmBridgeChannel));
            }

            TvmBridge_ = NAuth::CreateTvmBridge(GetControlInvoker(), std::move(tvmBridgeChannel), Config_->TvmBridge);
            NAuth::TNativeAuthenticationManager::Get()->SetTvmService(TvmBridge_);
        }

        SolomonExporter_ = New<TSolomonExporter>(Config_->SolomonExporter);

        environment = CreateJobProxyEnvironment(
            Config_,
            JobThread_->GetInvoker(),
            GetSlotPath(),
            /*failedSidecarCallback*/ [this] (TError sidecarError) {
                auto job = FindJob();
                if (!job) {
                    YT_LOG_FATAL("Job is missing within sidecar failure (SidecarError: %v)", sidecarError);
                }
                job->Fail(std::move(sidecarError));
            });
        SetJobProxyEnvironment(environment);

        LocalDescriptor_ = NNodeTrackerClient::TNodeDescriptor(Config_->Addresses, Config_->LocalHostName, Config_->Rack, Config_->DataCenter);

        TrafficMeter_ = New<TTrafficMeter>(LocalDescriptor_.GetDataCenter());
        TrafficMeter_->Start();

        YT_VERIFY(Config_->BusServer->UnixDomainSocketPath);
        YT_VERIFY(Config_->GrpcServer->Addresses.size() == 1);

        InitializeOrchid();

        auto jobApiService = CreateJobApiService(
            Config_->JobApiService,
            GetControlInvoker(),
            MakeWeak(this));

        YT_LOG_INFO(
            "Creating RPC and GRPC servers (RpcSocketPath: %v, GrpcSocketPath: %v)",
            Config_->BusServer->UnixDomainSocketPath,
            Config_->GrpcServer->Addresses[0]->Address);

        RpcServer_ = NRpc::NBus::CreateBusServer(CreateLocalTcpBusServer(Config_->BusServer));
        RpcServer_->RegisterService(CreateJobProberService(this, GetControlInvoker()));
        RpcServer_->RegisterService(jobApiService);
        RpcServer_->RegisterService(NOrchid::CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker(),
            /*authenticator*/ nullptr));

        RpcServer_->Start();

        if (Config_->EnableGrpcServer) {
            GrpcServer_ = NRpc::NGrpc::CreateServer(Config_->GrpcServer);
            GrpcServer_->RegisterService(jobApiService);
            GrpcServer_->Start();
        }

        if (Config_->EnableHttpServer) {
            auto address = TNetworkAddress::CreateUnixDomainSocketAddress(NFS::GetShortestPath(Config_->HttpServerUdsPath));
            auto poller = CreateThreadPoolPoller(Config_->HttpServerPollerThreadCount, Config_->HttpServer->ServerName);
            auto acceptor = poller;
            HttpServer_ = NRpc::NHttp::CreateServer(::NYT::NHttp::CreateServer(
                Config_->HttpServer,
                CreateListener(address, poller, acceptor),
                poller));
            HttpServer_->RegisterService(jobApiService);
            HttpServer_->Start();
        }

        if (TvmBridge_) {
            YT_LOG_DEBUG("Ensuring destination service id (ServiceId: %v)", TvmBridge_->GetSelfTvmId());

            WaitFor(TvmBridge_->EnsureDestinationServiceIds({TvmBridge_->GetSelfTvmId()}))
                .ThrowOnError();

            YT_LOG_DEBUG("Destination service id is ready");
        }

        auto supervisorClient = CreateBusClient(Config_->SupervisorConnection);
        auto supervisorChannel = NRpc::NBus::CreateBusChannel(supervisorClient);
        if (TvmBridge_) {
            auto serviceTicketAuth = CreateServiceTicketAuth(TvmBridge_, TvmBridge_->GetSelfTvmId());
            supervisorChannel = NAuth::CreateServiceTicketInjectingChannel(
                std::move(supervisorChannel),
                {.ServiceTicketAuth = serviceTicketAuth});
        }

        if (Config_->UseRetryingChannels) {
            supervisorChannel = CreateRetryingChannel(
                Config_->RetryingChannel,
                std::move(supervisorChannel));
        }

        if (Config_->DnsOverRpcResolver) {
            YT_LOG_INFO("Installing DNS-over-RPC resolver");
            auto dnsResolver = NDns::CreateDnsOverRpcResolver(Config_->DnsOverRpcResolver, supervisorChannel);
            NNet::TAddressResolver::Get()->SetDnsResolver(std::move(dnsResolver));
            // This is to enable testing the feature.
            NNet::TAddressResolver::Get()->PurgeCache();
        }

        SupervisorChannel_ = supervisorChannel;
        SupervisorProxy_ = std::make_unique<TSupervisorServiceProxy>(SupervisorChannel_);
        SupervisorProxy_->SetDefaultTimeout(Config_->SupervisorRpcTimeout);

        RetrieveJobSpec();

        auto clusterConnection = CreateNativeConnection(Config_->ClusterConnection);
        Client_ = clusterConnection->CreateNativeClient(NNative::TClientOptions::FromUser(GetAuthenticatedUser()));

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(Client_);

        PackBaggageFromJobSpec(RootSpan_, JobSpecHelper_->GetJobSpec(), OperationId_, JobId_, GetJobSpecHelper()->GetJobType());

        auto cpuMonitorConfig = ConvertTo<TJobCpuMonitorConfigPtr>(TYsonString(JobSpecHelper_->GetJobSpecExt().job_cpu_monitor_config()));
        CpuMonitor_ = New<TCpuMonitor>(std::move(cpuMonitorConfig), JobThread_->GetInvoker(), this, CpuGuarantee_);

        if (Config_->JobThrottler) {
            YT_LOG_INFO("Job throttling enabled");

            // InBandwidthThrottlers are created on demand.

            OutBandwidthThrottler_ = CreateOutJobBandwidthThrottler(
                Config_->JobThrottler,
                SupervisorChannel_,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_,
                Logger);

            OutRpsThrottler_ = CreateOutJobRpsThrottler(
                Config_->JobThrottler,
                SupervisorChannel_,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_,
                Logger);

            UserJobContainerCreationThrottler_ = CreateUserJobContainerCreationThrottler(
                Config_->JobThrottler,
                SupervisorChannel_,
                GetJobSpecHelper()->GetJobIOConfig()->TableWriter->WorkloadDescriptor,
                JobId_);
        } else {
            YT_LOG_INFO("Job throttling disabled");

            // InBandwidthThrottlers are created on demand.

            OutBandwidthThrottler_ = GetUnlimitedThrottler();
            OutRpsThrottler_ = GetUnlimitedThrottler();
            UserJobContainerCreationThrottler_ = GetUnlimitedThrottler();
        }

        const auto& jobSpecExt = GetJobSpecHelper()->GetJobSpecExt();
        if (jobSpecExt.is_traced()) {
            RootSpan_->SetSampled();
        }
        if (jobSpecExt.has_user_job_spec()) {
            const auto& userJobSpec = jobSpecExt.user_job_spec();
            if (userJobSpec.enable_rpc_proxy_in_job_proxy()) {
                EnableRpcProxyInJobProxy(userJobSpec.rpc_proxy_worker_thread_pool_size(), userJobSpec.enable_shuffle_service_in_job_proxy());
            } else {
                YT_VERIFY(!userJobSpec.enable_shuffle_service_in_job_proxy());
            }
        }

        JobProxyMemoryOvercommitLimit_ = jobSpecExt.has_job_proxy_memory_overcommit_limit()
            ? std::make_optional(jobSpecExt.job_proxy_memory_overcommit_limit())
            : std::nullopt;

        RefCountedTrackerLogPeriod_ = FromProto<TDuration>(jobSpecExt.job_proxy_ref_counted_tracker_log_period());

        if (environment) {
            environment->SetCpuGuarantee(CpuGuarantee_);
            if (jobSpecExt.has_user_job_spec() &&
                jobSpecExt.user_job_spec().set_container_cpu_limit())
            {
                auto limit = jobSpecExt.user_job_spec().container_cpu_limit() > 0
                    ? jobSpecExt.user_job_spec().container_cpu_limit()
                    : CpuGuarantee_.load();
                environment->SetCpuLimit(limit);
            }

            if (Config_->ForceIdleCpuPolicy) {
                environment->SetCpuPolicy("idle");
            }
        }

        HeartbeatExecutor_ = New<TPeriodicExecutor>(
            JobThread_->GetInvoker(),
            BIND(&TJobProxy::SendHeartbeat, MakeWeak(this)),
            Config_->HeartbeatPeriod);

        MemoryWatchdogExecutor_ = New<TPeriodicExecutor>(
            JobThread_->GetInvoker(),
            BIND(&TJobProxy::CheckMemoryUsage, MakeWeak(this)),
            Config_->JobEnvironment->MemoryWatchdogPeriod);

        if (jobSpecExt.has_user_job_spec()) {
            job = CreateUserJob(
                this,
                jobSpecExt.user_job_spec(),
                JobId_,
                GetJobSpecHelper()->GetJobType(),
                Ports_,
                std::make_unique<TUserJobWriteController>(this));
        } else {
            job = CreateBuiltinJob();
        }

        if (GetJobSpecHelper()->GetJobSpecExt().remote_input_clusters_size() > 0) {
            // NB(coteeq): Do not sync cluster directory if data is local only.
            auto connection = GetClient()->GetNativeConnection();
            WaitFor(connection->GetClusterDirectorySynchronizer()->Sync())
                .ThrowOnError();
        }

        InitializeChunkReaderHost();
        SetJob(job);
        job->Initialize();

        OnSpawned();
    } catch (const std::exception& ex) {
        auto isSupervisorProxyTimeoutError = [] (const TError& error) {
            auto serviceAttribute = error.Attributes().Find<std::optional<TString>>("service");
            return error.GetCode() == NYT::EErrorCode::Timeout &&
                serviceAttribute == TSupervisorServiceProxy::GetDescriptor().ServiceName;
        };

        YT_LOG_ERROR(ex, "Failed to prepare job proxy");
        auto error = TError(ex);
        if (error.FindMatching(isSupervisorProxyTimeoutError)) {
            Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
        }
        Abort(EJobProxyExitCode::JobProxyPrepareFailed);
    }

    if (Config_->TestingConfig->FailPreparation) {
        Abort(EJobProxyExitCode::JobProxyPrepareFailed);
    }

    job->PrepareArtifacts();
    OnArtifactsPrepared();

    if (GetJobSpecHelper()->GetJobTestingOptions()->FailBeforeJobStart) {
        THROW_ERROR_EXCEPTION("Fail before job started");
    }

    MemoryWatchdogExecutor_->Start();
    HeartbeatExecutor_->Start();
    CpuMonitor_->Start();

    if (GetJobSpecHelper()->HasSidecars()) {
        environment->StartSidecars(GetJobSpecHelper()->GetJobSpecExt());
    }

    if (auto eventsOnFsConfig = Config_->JobTestingOptions->EventsOnFs) {
        WaitFor(
            GetBreakpointEvent(
                eventsOnFsConfig,
                JobId_,
                EBreakpointType::BeforeRun))
            .ThrowOnError();
    }

    return job->Run();
}

NApi::NNative::IConnectionPtr TJobProxy::CreateNativeConnection(
    NApi::NNative::TConnectionCompoundConfigPtr config,
    NApi::NNative::TConnectionOptions options) const
{
    if (TvmBridge_ && config->Dynamic->TvmId) {
        YT_LOG_DEBUG("Ensuring destination service id (ServiceId: %v)", *config->Dynamic->TvmId);

        WaitFor(TvmBridge_->EnsureDestinationServiceIds({*config->Dynamic->TvmId}))
            .ThrowOnError();

        YT_LOG_DEBUG("Destination service id is ready");
    }

    options.RetryRequestQueueSizeLimitExceeded = true;

    return NApi::NNative::CreateConnection(std::move(config), std::move(options));
}

void TJobProxy::ReportResult(
    const TJobResult& result,
    TInstant startTime,
    TInstant finishTime)
{
    TTraceContextGuard guard(RootSpan_);

    YT_LOG_INFO("Reporting job result");

    if (!SupervisorProxy_) {
        YT_LOG_ERROR("Supervisor channel is not available");
        Abort(EJobProxyExitCode::ResultReportFailed);
    }

    auto req = SupervisorProxy_->OnJobFinished();
    ToProto(req->mutable_job_id(), JobId_);
    *req->mutable_result() = result;
    req->set_start_time(ToProto(startTime));
    req->set_finish_time(ToProto(finishTime));
    auto job = FindJob();
    if (job) {
        FillStatistics(req, job, GetEnrichedStatistics());
    }

    // Check that data statistics do not contradict with the actual
    // resulting chunk specs.
    // NB(coteeq): RemoteCopy jobs do not send chunk specs to the controller.
    // NB(coteeq): If job finished with an error, the check may not be applicable.
    if (GetJobSpecHelper()->GetJobType() != EJobType::RemoteCopy && !result.has_error()){
        const auto& jobResultExt = result.GetExtension(TJobResultExt::job_result_ext);
        i64 totalChunkCount = 0;
        for (const auto& statistics : req->output_data_statistics()) {
            totalChunkCount += statistics.chunk_count();
        }
        YT_VERIFY((totalChunkCount > 0) == (jobResultExt.output_chunk_specs_size() > 0));
    }

    if (job && GetJobSpecHelper()->GetJobSpecExt().has_user_job_spec()) {
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
            auto stderr = GetStderr({});
            if (!stderr.Data.empty()) {
                const auto& jobResultExt = result.GetExtension(TJobResultExt::job_result_ext);
                YT_VERIFY(jobResultExt.has_stderr());
            }
            req->set_job_stderr(stderr.Data.data(), stderr.Data.size());
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job stderr on teardown");
        }
    }

    if (auto job = FindJob()) {
        try {
            for (const auto& profile : job->GetProfiles()) {
                auto* protoProfile = req->add_profiles();
                protoProfile->set_type(profile.Type);
                protoProfile->set_blob(profile.Blob);
                protoProfile->set_profiling_probability(profile.ProfilingProbability);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get job profile on teardown");
        }
    }

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        YT_LOG_ERROR(rspOrError, "Failed to report job result");
        Abort(EJobProxyExitCode::ResultReportFailed);
    }

    YT_LOG_INFO("Job result reported");
}

void TJobProxy::InitializeChunkReaderHost()
{
    auto bandwidthThrottlerProvider = BIND([this, weakThis = MakeWeak(this)] (const TClusterName& clusterName) -> TPerCategoryThrottlerProvider {
        return BIND([=, this] (EWorkloadCategory /*category*/) -> IThroughputThrottlerPtr {
            auto this_ = weakThis.Lock();
            if (!this_) {
                return GetUnlimitedThrottler();
            }

            return JobSpecHelper_->GetJobSpecExt().use_cluster_throttlers()
                ? GetInBandwidthThrottler(clusterName)
                : GetInBandwidthThrottler(LocalClusterName);
        });
    });

    std::vector<TMultiChunkReaderHost::TClusterContext> clusterContextList{
        {
            .Name = LocalClusterName,
            .Client = Client_,
            .ChunkReaderStatistics = New<TChunkReaderStatistics>(),
        },
    };
    for (const auto& [remoteClusterName, protoRemoteCluster] : GetJobSpecHelper()->GetJobSpecExt().remote_input_clusters()) {
        auto remoteConnection = Client_
            ->GetNativeConnection()
            ->GetClusterDirectory()
            ->GetConnectionOrThrow(remoteClusterName);

        if (!protoRemoteCluster.networks().empty()) {
            auto connectionConfig = remoteConnection
                ->GetCompoundConfig()
                ->Clone();
            connectionConfig->Static->Networks = FromProto<NNodeTrackerClient::TNetworkPreferenceList>(
                protoRemoteCluster.networks());
            remoteConnection = CreateNativeConnection(std::move(connectionConfig));
        }

        remoteConnection->GetNodeDirectory()->MergeFrom(protoRemoteCluster.node_directory());

        clusterContextList.push_back(
            TMultiChunkReaderHost::TClusterContext{
                .Name = TClusterName(remoteClusterName),
                .Client = remoteConnection->CreateNativeClient(Client_->GetOptions()),
                .ChunkReaderStatistics = New<TChunkReaderStatistics>(),
            });
    }

    MultiChunkReaderHost_ = New<TMultiChunkReaderHost>(
        New<TChunkReaderHost>(
            Client_,
            LocalDescriptor_,
            ReaderBlockCache_,
            /*chunkMetaCache*/ nullptr,
            bandwidthThrottlerProvider(LocalClusterName),
            GetOutRpsThrottler(),
            /*mediumThrottler*/ nullptr,
            GetTrafficMeter()),
        bandwidthThrottlerProvider,
        clusterContextList);
}

TStatistics TJobProxy::GetEnrichedStatistics() const
{
    TStatistics statistics;

    auto statisticsOutputTableCountLimit = Config_->StatisticsOutputTableCountLimit.value_or(std::numeric_limits<int>::max());

    if (auto job = FindJob()) {
        auto extendedStatistics = job->GetStatistics();
        statistics = std::move(extendedStatistics.Statistics);

        if (job->HasInput()) {
            statistics.AddSample("/data/input"_SP, extendedStatistics.TotalInputStatistics.DataStatistics);
            DumpCodecStatistics(extendedStatistics.TotalInputStatistics.CodecStatistics, "/codec/cpu/decode"_SP, &statistics);
        }

        for (int index = 0; index < std::min<int>(statisticsOutputTableCountLimit, extendedStatistics.OutputStatistics.size()); ++index) {
            auto ypathIndex = TStatisticPathLiteral(ToString(index));
            statistics.AddSample("/data/output"_SP / ypathIndex, extendedStatistics.OutputStatistics[index].DataStatistics);
            DumpCodecStatistics(extendedStatistics.OutputStatistics[index].CodecStatistics, "/codec/cpu/encode"_SP / ypathIndex, &statistics);
        }

        auto totalChunkReaderStatistics = New<TChunkReaderStatistics>();
        if (GetJobSpecHelper()->GetJobType() == EJobType::RemoteCopy) {
            // Chunk reader statistics for remote copy is collected in a custom way.
            if (auto chunkReaderStatistics = extendedStatistics.RemoteCopyChunkReaderStatistics) {
                auto remoteCopyJobSpecExt = GetJobSpecHelper()->GetJobSpec()
                    .GetExtension(TRemoteCopyJobSpecExt::remote_copy_job_spec_ext);

                if (remoteCopyJobSpecExt.has_remote_cluster_name() &&
                    Config_->EnablePerClusterChunkReaderStatistics &&
                    Config_->DumpSingleLocalClusterStatistics)
                {
                    TClusterName clusterName{remoteCopyJobSpecExt.remote_cluster_name()};
                    YT_VERIFY(!IsLocal(clusterName));

                    DumpChunkReaderStatistics(
                        &statistics,
                        "/chunk_reader_statistics"_SP / TStatisticPathLiteral(ToStringViaBuilder(clusterName)),
                        chunkReaderStatistics);
                }

                totalChunkReaderStatistics->AddFrom(chunkReaderStatistics);
            }
        } else {
            bool shouldDumpLocalStatistics = MultiChunkReaderHost_->GetChunkReaderStatistics().size() > 1u || Config_->DumpSingleLocalClusterStatistics;
            for (const auto& [clusterName, chunkReaderStatistics] : MultiChunkReaderHost_->GetChunkReaderStatistics()) {
                bool shouldDump = Config_->EnablePerClusterChunkReaderStatistics && (!IsLocal(clusterName) || shouldDumpLocalStatistics);
                if (shouldDump) {
                    DumpChunkReaderStatistics(
                        &statistics,
                        "/chunk_reader_statistics"_SP / TStatisticPathLiteral(ToStringViaBuilder(clusterName)),
                        chunkReaderStatistics);
                }
                totalChunkReaderStatistics->AddFrom(chunkReaderStatistics);
            }
        }

        DumpChunkReaderStatistics(&statistics, "/chunk_reader_statistics"_SP, totalChunkReaderStatistics);
        DumpTimingStatistics(&statistics, "/chunk_reader_statistics"_SP, extendedStatistics.TimingStatistics);

        for (int index = 0; index < std::min<int>(statisticsOutputTableCountLimit, extendedStatistics.ChunkWriterStatistics.size()); ++index) {
            auto ypathIndex = TStatisticPathLiteral(ToString(index));
            DumpChunkWriterStatistics(&statistics, "/chunk_writer_statistics"_SP / ypathIndex, extendedStatistics.ChunkWriterStatistics[index]);
        }

        if (const auto& pipeStatistics = extendedStatistics.PipeStatistics) {
            auto dumpPipeStatistics = [&] (const TStatisticPath& path, const IJob::TStatistics::TPipeStatistics& pipeStatistics) {
                statistics.AddSample(path / "idle_time"_L, pipeStatistics.ConnectionStatistics.IdleDuration);
                statistics.AddSample(path / "busy_time"_L, pipeStatistics.ConnectionStatistics.BusyDuration);
                statistics.AddSample(path / "bytes"_L, pipeStatistics.Bytes);
            };

            if (job->HasInput()) {
                dumpPipeStatistics("/user_job/pipes/input"_SP, *pipeStatistics->InputPipeStatistics);
            }
            dumpPipeStatistics("/user_job/pipes/output/total"_SP, pipeStatistics->TotalOutputPipeStatistics);
            for (int index = 0; index < std::min<int>(statisticsOutputTableCountLimit, pipeStatistics->OutputPipeStatistics.size()); ++index) {
                dumpPipeStatistics("/user_job/pipes/output"_SP / TStatisticPathLiteral(ToString(index)), pipeStatistics->OutputPipeStatistics[index]);
            }
        }

        if (auto time = extendedStatistics.LatencyStatistics.InputTimeToFirstReadBatch) {
            statistics.AddSample("/latency/input/time_to_first_read_batch"_SP, *time);
        }
        if (auto time = extendedStatistics.LatencyStatistics.InputTimeToFirstWrittenBatch) {
            statistics.AddSample("/latency/input/time_to_first_written_batch"_SP, *time);
        }

        TDuration minOutputTimeToFirstBatch = TDuration::Max();
        for (const auto& [index, time] : Enumerate(extendedStatistics.LatencyStatistics.OutputTimeToFirstReadBatch)) {
            if (!time) {
                continue;
            }
            minOutputTimeToFirstBatch = std::min(minOutputTimeToFirstBatch, *time);
            statistics.AddSample(
                "/latency/output"_SP / TStatisticPathLiteral(ToString(index)) / "time_to_first_read_batch"_L,
                *time);
        }

        if (minOutputTimeToFirstBatch != TDuration::Max()) {
            statistics.AddSample(
                "/latency/output/total/min_time_to_first_read_batch"_SP,
                minOutputTimeToFirstBatch);
        }

        for (const auto& [index, timingStatistics] : SEnumerate(extendedStatistics.WriterTimingStatistics)) {
            statistics.AddSample(
                "/chunk_writer_statistics"_SP / TStatisticPathLiteral(ToString(index)) / "write_time"_L,
                timingStatistics.WriteTime);
            statistics.AddSample(
                "/chunk_writer_statistics"_SP / TStatisticPathLiteral(ToString(index)) / "wait_time"_L,
                timingStatistics.WaitTime);
            statistics.AddSample(
                "/chunk_writer_statistics"_SP / TStatisticPathLiteral(ToString(index)) / "idle_time"_L,
                timingStatistics.IdleTime);
            statistics.AddSample(
                "/chunk_writer_statistics"_SP / TStatisticPathLiteral(ToString(index)) / "close_time"_L,
                timingStatistics.CloseTime);
        }
    }

    if (auto environment = FindJobProxyEnvironment()) {
        try {
            auto cpuStatistics = environment->GetCpuStatistics()
                .ValueOrThrow();
            statistics.AddSample("/job_proxy/cpu"_SP, cpuStatistics);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unable to get CPU statistics from resource controller");
        }

        try {
            auto blockIOStatistics = environment->GetBlockIOStatistics()
                .ValueOrThrow();
            statistics.AddSample("/job_proxy/block_io"_SP, blockIOStatistics);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Unable to get block IO statistics from resource controller");
        }

        // NB(arkady-e1ppa): GetXx methods are noexcept
        // AddSample methods can only throw if path is incorrect.
        // This is impossible for a hardcoded path.
        // If you happen to change path, don't forget to
        // change it here as well and make it compatible.
        RunNoExcept([&] {
            auto jobCpuStatistics = environment->GetJobCpuStatistics();
            if (jobCpuStatistics) {
                statistics.AddSample("/job/cpu"_SP, *jobCpuStatistics);
            }

            auto jobMemoryStatistics = environment->GetJobMemoryStatistics();
            if (jobMemoryStatistics) {
                statistics.AddSample("/job/memory"_SP, *jobMemoryStatistics);
            }

            auto jobBlockIOStatistics = environment->GetJobBlockIOStatistics();
            if (jobBlockIOStatistics) {
                statistics.AddSample("/job/block_io"_SP, *jobBlockIOStatistics);
            }
        });
    }

    if (JobProxyMaxMemoryUsage_ > 0) {
        statistics.AddSample("/job_proxy/max_memory"_SP, JobProxyMaxMemoryUsage_);
    }

    if (JobProxyMemoryReserve_ > 0) {
        statistics.AddSample("/job_proxy/memory_reserve"_SP, JobProxyMemoryReserve_);
    }

    if (CumulativeMemoryUsageMBSec_ > 0) {
        statistics.AddSample("/job_proxy/cumulative_memory_mb_sec"_SP, CumulativeMemoryUsageMBSec_);
    }

    FillTrafficStatistics("/job_proxy"_SP, statistics, TrafficMeter_);

    CpuMonitor_->FillStatistics(statistics);

    statistics.SetTimestamp(TInstant::Now());

    return statistics;
}

IUserJobEnvironmentPtr TJobProxy::CreateUserJobEnvironment(const TJobSpecEnvironmentOptions& options) const
{
    YT_LOG_DEBUG("Creating user job environment");

    auto environment = FindJobProxyEnvironment();
    YT_VERIFY(environment);

    auto createRootFS = [&] () -> std::optional<TRootFS> {
        TString stderrPath;
        if (Config_->ExecutorStderrPath) {
            stderrPath = *Config_->ExecutorStderrPath;
        } else {
            stderrPath = NFS::CombinePaths(
                GetPreparationPath(),
                DefaultExecutorStderrPath);
        }

        if (!NFS::Exists(stderrPath)) {
            NFS::MakeDirRecursive(NFS::GetDirectoryName(stderrPath));
            TFile file(stderrPath, CreateNew | WrOnly);
        }

        NFS::SetPermissions(
            stderrPath,
            /*permissions*/ 0666);

        if (!Config_->RootPath) {
            YT_LOG_INFO("Job is not using custom rootfs");
            return std::nullopt;
        }

        if (Config_->TestRootFS) {
            YT_LOG_INFO("Job is running in testing rootfs mode");
            return std::nullopt;
        }

        YT_LOG_DEBUG("Job is using custom rootfs (Path: %v)", Config_->RootPath);

        TRootFS rootFS {
            .RootPath = *Config_->RootPath,
            .IsRootReadOnly = false,
        };

        auto preparationPath = GetPreparationPath();

        // Temporary workaround for nirvana - make tmp directories writable.
        auto tmpPath = NFS::CombinePaths(
            preparationPath,
            GetSandboxRelPath(ESandboxKind::Tmp));

        rootFS.Binds.push_back(TBind{
            .SourcePath = TString(tmpPath),
            .TargetPath = "/tmp",
            .ReadOnly = false,
        });

        rootFS.Binds.push_back(TBind{
            .SourcePath = TString(tmpPath),
            .TargetPath = "/var/tmp",
            .ReadOnly = false,
        });

        if (Config_->EnableRootVolumeDiskQuota) {
            auto slotPath = GetSlotPath();

            auto addBind = [&] (ESandboxKind sandboxKind) {
                auto sandboxRelPath = GetSandboxRelPath(sandboxKind);
                rootFS.Binds.push_back(TBind{
                    .SourcePath = NFS::CombinePaths(
                        preparationPath,
                        sandboxRelPath),
                    .TargetPath = NFS::CombinePaths(
                        slotPath,
                        sandboxRelPath),
                    .ReadOnly = false,
                });
            };

            addBind(ESandboxKind::Home);
            addBind(ESandboxKind::Cores);
            addBind(ESandboxKind::Pipes);
            addBind(ESandboxKind::Logs);

            rootFS.Binds.push_back(TBind{
                .SourcePath = NFS::CombinePaths(
                    preparationPath,
                    ExecutorConfigFileName),
                .TargetPath = NFS::CombinePaths(
                    slotPath,
                    ExecutorConfigFileName),
                .ReadOnly = false,
            });
        }

        // Mount sandbox home to user homedir.
        {
            const auto& source = NFS::CombinePaths(
                GetPreparationPath(),
                GetSandboxRelPath(ESandboxKind::Home));
            const auto& target = Format("/home/yt_slot_%d", Config_->SlotIndex);

            YT_LOG_DEBUG("Adding container bind for slot home (Source: %v, Target: %v)",
                source,
                target);

            rootFS.Binds.push_back(TBind{
                .SourcePath = TString(source),
                .TargetPath = TString(target),
                .ReadOnly = false,
            });
        }

        for (const auto& bind : Config_->Binds) {
            YT_LOG_DEBUG("Adding container bind for config binds (Source: %v, Target: %v)",
                bind->ExternalPath,
                bind->InternalPath);

            rootFS.Binds.push_back(TBind{
                .SourcePath = bind->ExternalPath,
                .TargetPath = bind->InternalPath,
                .ReadOnly = bind->ReadOnly,
            });
        }

        if (Config_->ExecutorStderrPath) {
            rootFS.Binds.push_back(TBind{
                .SourcePath = stderrPath,
                .TargetPath = stderrPath,
                .ReadOnly = false,
            });
        } else {
            rootFS.Binds.push_back(TBind{
                .SourcePath = stderrPath,
                .TargetPath = NFS::CombinePaths(
                    GetSlotPath(),
                    DefaultExecutorStderrPath),
                .ReadOnly = false,
            });
        }

        // TODO(gritukan): ytserver-exec can be resolved into something strange in tests,
        // so let's live with exec in layer for a while.
        if (!JobProxyEnvironment_.Acquire()->UseExecFromLayer()) {
            rootFS.Binds.push_back(TBind{
                .SourcePath = ResolveBinaryPath(ExecProgramName).ValueOrThrow(),
                .TargetPath = RootFSBinaryDirectory + ExecProgramName,
                .ReadOnly = true,
            });
        }

        return rootFS;
    };

    TUserJobEnvironmentOptions environmentOptions {
        .RootFS = createRootFS(),
        .GpuIndexes = Config_->GpuIndexes,
        .HostName = Config_->HostName,
        .NetworkAddresses = Config_->NetworkAddresses,
        .EnableNat64 = Config_->EnableNat64,
        .DisableNetwork = Config_->DisableNetwork,
        .EnableFuse = Config_->EnableFuse,
        .EnableCudaGpuCoreDump = options.EnableGpuCoreDumps,
        .EnablePortoMemoryTracking = options.EnablePortoMemoryTracking,
        .EnablePorto = options.EnablePorto,
        .ThreadLimit = options.ThreadLimit,
        .EnableRootVolumeDiskQuota = Config_->EnableRootVolumeDiskQuota,
    };

    if (Config_->RestrictPortoPlace) {
        environmentOptions.Places.push_back(NFS::CombinePaths(Config_->SlotPath, "place"));
    } else {
        environmentOptions.Places.push_back(NFS::CombinePaths(Config_->SlotPath, "place"));
        // COMPAT(yuryalekseev): Remove this after tasklets move to using default place.
        environmentOptions.Places.push_back(AnyTarget);
    }

    if (options.EnableCoreDumps) {
        environmentOptions.SlotCoreWatcherDirectory = NFS::CombinePaths({GetSlotPath(), "cores"});
        environmentOptions.CoreWatcherDirectory = NFS::CombinePaths({GetPreparationPath(), "cores"});
    }

    return environment->CreateUserJobEnvironment(
        JobId_,
        environmentOptions);
}

TJobProxyInternalConfigPtr TJobProxy::GetConfig() const
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

TString TJobProxy::GetAuthenticatedUser() const
{
    return JobSpecHelper_->GetJobSpecExt().authenticated_user();
}

std::string TJobProxy::GetLocalHostName() const
{
    return Config_->LocalHostName;
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
    // TODO(pogorelov): Looks like reordering could happen here. Fix it.
    req->Invoke().Subscribe(BIND(&TJobProxy::OnResourcesUpdated, MakeWeak(this), RequestedMemoryReserve_.load()));
}

void TJobProxy::OnSpawned()
{
    if (Config_->TestingConfig->FailOnJobProxySpawnedCall) {
        YT_LOG_ERROR("Fail OnJobProxySpawned call for testing purposes");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }

    auto req = SupervisorProxy_->OnJobProxySpawned();
    ToProto(req->mutable_job_id(), JobId_);
    auto error = WaitFor(req->Invoke());
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Failed to notify supervisor about job proxy spawned");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }
}

void TJobProxy::OnArtifactsPrepared()
{
    auto req = SupervisorProxy_->OnArtifactsPrepared();
    ToProto(req->mutable_job_id(), JobId_);
    auto error = WaitFor(req->Invoke());
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Failed to notify supervisor about artifacts preparation");
        Abort(EJobProxyExitCode::SupervisorCommunicationFailed);
    }
}

void TJobProxy::SetUserJobMemoryUsage(i64 memoryUsage)
{
    UserJobCurrentMemoryUsage_ = memoryUsage;
}

void TJobProxy::OnResourcesUpdated(i64 memoryReserve, const TError& error)
{
    if (!error.IsOK()) {
        YT_LOG_ERROR(error, "Failed to update resource usage");
        Abort(EJobProxyExitCode::ResourcesUpdateFailed);
    }

    if (ApprovedMemoryReserve_ < memoryReserve) {
        YT_LOG_DEBUG("Successfully updated resource usage (MemoryReserve: %v)", memoryReserve);
        ApprovedMemoryReserve_ = memoryReserve;
    }
}

void TJobProxy::ReleaseNetwork()
{
    YT_LOG_INFO("Releasing network");
    NetworkUsage_ = 0;
    UpdateResourceUsage();
}

void TJobProxy::OnPrepared()
{
    YT_LOG_INFO("Job prepared");

    auto req = SupervisorProxy_->OnJobPrepared();
    ToProto(req->mutable_job_id(), JobId_);
    YT_UNUSED_FUTURE(req->Invoke().Apply(BIND(
        [this, this_ = MakeStrong(this)] (const TSupervisorServiceProxy::TErrorOrRspOnJobPreparedPtr& /*rspOrError*/) {
            Prepared_.store(true);
        })));
}

void TJobProxy::PrepareArtifact(
    const TString& artifactName,
    const TString& pipePath)
{
    YT_LOG_INFO("Requesting node to prepare artifact (ArtifactName: %v, PipePath: %v)",
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
    YT_LOG_ERROR(error, "Artifact preparation failed (ArtifactName: %v, ArtifactPath: %v)",
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

void TJobProxy::OnJobMemoryThrashing()
{
    auto req = SupervisorProxy_->OnJobMemoryThrashing();
    ToProto(req->mutable_job_id(), JobId_);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

NApi::NNative::IClientPtr TJobProxy::GetClient() const
{
    return Client_;
}

const TMultiChunkReaderHostPtr& TJobProxy::GetChunkReaderHost() const
{
    return MultiChunkReaderHost_;
}

IBlockCachePtr TJobProxy::GetReaderBlockCache() const
{
    return ReaderBlockCache_;
}

IBlockCachePtr TJobProxy::GetWriterBlockCache() const
{
    return GetNullBlockCache();
}

const NNodeTrackerClient::TNodeDescriptor& TJobProxy::LocalDescriptor() const
{
    return LocalDescriptor_;
}

void TJobProxy::CheckMemoryUsage()
{
    i64 jobProxyMemoryUsage = 0;
    try {
        auto memoryUsage = GetProcessMemoryUsage();
        // RSS from /proc/pid/statm includes all pages resident to current process,
        // including memory-mapped files and shared memory.
        // Since there typically are multiple instances of job proxy on the host sharing the same binary
        // we don't want to account shared pages overhead on every job. Moreover, this memory is not accounted by
        // controller agent during memory consumption estimation.
        jobProxyMemoryUsage = memoryUsage.Rss - memoryUsage.Shared;
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get process memory usage");
        return;
    }

    if (Config_->OomScoreAdjOnExceededMemoryReserve.has_value()) {
        int targetOomScore = jobProxyMemoryUsage > JobProxyMemoryReserve_ ? *Config_->OomScoreAdjOnExceededMemoryReserve : 0;
        SetOomScoreAdj(targetOomScore);
    }

    JobProxyMaxMemoryUsage_ = std::max(JobProxyMaxMemoryUsage_.load(), jobProxyMemoryUsage);
    UpdateCumulativeMemoryUsage(jobProxyMemoryUsage);

    YT_LOG_DEBUG("Job proxy memory check (JobProxyMemoryUsage: %v, JobProxyMaxMemoryUsage: %v, JobProxyMemoryReserve: %v, UserJobCurrentMemoryUsage: %v)",
        jobProxyMemoryUsage,
        JobProxyMaxMemoryUsage_.load(),
        JobProxyMemoryReserve_,
        UserJobCurrentMemoryUsage_.load());

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
                TRefCountedTracker::Get()->GetDebugInfo(2 /*sortByColumn*/));
            // NB(coteeq): Refcount dump and TCMalloc stats are quite huge, so we will exceed
            // logging's message length limit. So let's duplicate the warning to reliably see at least
            // a prefix of both blobs.
            YT_LOG_WARNING("Job proxy used more memory than estimated "
                "(JobProxyMaxMemoryUsage: %v, JobProxyMemoryReserve: %v, TCMallocStats: %v)",
                usage,
                JobProxyMemoryReserve_,
                tcmalloc::MallocExtension::GetStats());
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
            TRefCountedTracker::Get()->GetDebugInfo(2 /*sortByColumn*/));
    }

    i64 totalMemoryUsage = UserJobCurrentMemoryUsage_ + jobProxyMemoryUsage;

    const auto& jobSpecExt = JobSpecHelper_->GetJobSpecExt();
    if (jobSpecExt.has_user_job_spec()) {
        const auto& userJobSpec = jobSpecExt.user_job_spec();
        // NB: If job proxy is not prepared yet we cannot report actual statistics by heartbeat in abort.
        if (Config_->AlwaysAbortOnMemoryReserveOverdraft && Prepared_) {
            bool overdraft = false;
            if (UserJobCurrentMemoryUsage_ > userJobSpec.memory_reserve()) {
                YT_LOG_INFO("User job memory usage exceeded memory reserve (MemoryUsage: %v, MemoryReserve: %v)",
                    UserJobCurrentMemoryUsage_.load(),
                    userJobSpec.memory_reserve());
                overdraft = true;
            }
            if (jobProxyMemoryUsage > userJobSpec.job_proxy_memory_reserve()) {
                YT_LOG_INFO("Job proxy memory usage exceeded memory reserve (MemoryUsage: %v, MemoryReserve: %v)",
                    jobProxyMemoryUsage,
                    userJobSpec.job_proxy_memory_reserve());
                overdraft = true;
            }
            if (overdraft) {
                Abort(EJobProxyExitCode::ResourceOverdraft);
            }
        }
    }

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
        YT_LOG_DEBUG("Requesting node for memory usage increase (OldMemoryReserve: %v, NewMemoryReserve: %v)",
            RequestedMemoryReserve_.load(),
            memoryReserve);
        RequestedMemoryReserve_ = memoryReserve;
        UpdateResourceUsage();
    }
}

void TJobProxy::FillJobResult(TJobResult* jobResult)
{
    auto job = FindJob();
    if (!job) {
        return;
    }

    auto failedChunkIds = job->GetFailedChunkIds();
    if (!failedChunkIds.empty()) {
        YT_LOG_INFO("Failed chunks found (ChunkIds: %v)",
            failedChunkIds);
    }

    // For erasure chunks, replace part id with whole chunk id.
    auto* jobResultExt = jobResult->MutableExtension(TJobResultExt::job_result_ext);
    for (auto chunkId : failedChunkIds) {
        auto originalSpecIt = ChunkIdToOriginalSpec_.find(chunkId);
        auto originalChunkId = originalSpecIt.IsEnd()
            ? chunkId
            : FromProto<TChunkId>(originalSpecIt->second->chunk_id());
        auto actualChunkId = IsErasureChunkPartId(originalChunkId)
            ? ErasureChunkIdFromPartId(originalChunkId)
            : originalChunkId;
        ToProto(jobResultExt->add_failed_chunk_ids(), actualChunkId);
    }

    auto interruptDescriptor = job->GetInterruptDescriptor();
    PatchInterruptDescriptor(ChunkIdToOriginalSpec_, interruptDescriptor);

    if (!interruptDescriptor.UnreadDataSliceDescriptors.empty()) {
        auto inputStatistics = job->GetStatistics().TotalInputStatistics.DataStatistics;
        if (inputStatistics.row_count() > 0) {
            // NB(psushin): although we definitely have read some of the rows, the job may have made no progress,
            // since all of these row are from foreign tables, and therefor the ReadDataSliceDescriptors is empty.
            // Still we would like to treat such a job as interrupted, otherwise it may lead to an infinite sequence
            // of jobs being aborted by splitter instead of interrupts.

            NChunkClient::ToProto(
                jobResultExt->mutable_unread_chunk_specs(),
                jobResultExt->mutable_chunk_spec_count_per_unread_data_slice(),
                jobResultExt->mutable_virtual_row_index_per_unread_data_slice(),
                interruptDescriptor.UnreadDataSliceDescriptors);
            NChunkClient::ToProto(
                jobResultExt->mutable_read_chunk_specs(),
                jobResultExt->mutable_chunk_spec_count_per_read_data_slice(),
                jobResultExt->mutable_virtual_row_index_per_read_data_slice(),
                interruptDescriptor.ReadDataSliceDescriptors);

            jobResultExt->set_restart_needed(true);

            YT_LOG_INFO(
                "Interrupt descriptor found (UnreadDescriptorCount: %v, ReadDescriptorCount: %v, SchedulerResultExt: %v)",
                interruptDescriptor.UnreadDataSliceDescriptors.size(),
                interruptDescriptor.ReadDataSliceDescriptors.size(),
                jobResultExt->ShortDebugString());
        } else {
            if (jobResult->error().code() == 0) {
                ToProto(
                    jobResult->mutable_error(),
                    TError(NJobProxy::EErrorCode::InterruptionFailed, "Job did not read anything"));
            }
        }
    }

    const auto& jobSpecExt = GetJobSpecHelper()->GetJobSpecExt();
    if (jobSpecExt.has_user_job_spec()) {
        const auto& userJobSpec = jobSpecExt.user_job_spec();
        if (userJobSpec.has_restart_exit_code()) {
            auto error = FromProto<TError>(jobResult->error());
            if (auto userJobFailedError = error.FindMatching(NJobProxy::EErrorCode::UserJobFailed)) {
                auto processFailedError = userJobFailedError->FindMatching(EProcessErrorCode::NonZeroExitCode);
                if (processFailedError && processFailedError->Attributes().Get<int>("exit_code", 0) == userJobSpec.restart_exit_code()) {
                    YT_LOG_INFO("Job exited with code that indicates job restart (ExitCode: %v)",
                        userJobSpec.restart_exit_code());
                    jobResultExt->set_restart_needed(true);
                    ToProto(jobResult->mutable_error(), TError());
                }
            }
        }
    }
}

void TJobProxy::FillStderrResult(TJobResult* jobResult)
{
    const auto& jobSpecExt = GetJobSpecHelper()->GetJobSpecExt();
    const auto& userJobSpec = jobSpecExt.user_job_spec();

    auto* jobResultExt = jobResult->MutableExtension(TJobResultExt::job_result_ext);

    // If we were provided with stderr_table_spec we are expected to write stderr and provide some results.
    if (userJobSpec.has_stderr_table_spec() && !jobResultExt->has_stderr_result()) {
        // If error occurred during user job initialization, stderr blob table writer may not have been created at all.
        YT_LOG_WARNING("Stderr table boundary keys are absent");
        auto* stderrBoundaryKeys = jobResultExt->mutable_stderr_result();
        stderrBoundaryKeys->set_sorted(true);
        stderrBoundaryKeys->set_unique_keys(true);
        stderrBoundaryKeys->set_empty(true);
    }
}

void TJobProxy::Abort(EJobProxyExitCode exitCode)
{
    if (Config_->SendHeartbeatBeforeAbort) {
        auto error = WaitFor(HeartbeatExecutor_->GetExecutedEvent());
        YT_LOG_ERROR_UNLESS(error.IsOK(), error, "Failed to send heartbeat before abort");
    }

    if (auto job = FindJob()) {
        job->Cleanup();
    }

    TProgram::Abort(exitCode);
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

    auto validCpuStatistics = [] (const auto& cpuStats) {
        return
            cpuStats &&
            cpuStats->SystemUsageTime.has_value() &&
            cpuStats->UserUsageTime.has_value();
    };

    if (auto job = FindJob()) {
        auto userJobCpu = job->GetUserJobCpuStatistics();
        if (validCpuStatistics(userJobCpu)) {
            result += userJobCpu->SystemUsageTime.value() +
                userJobCpu->UserUsageTime.value();
        }
    }

    if (auto environment = FindJobProxyEnvironment()) {

        auto jobProxyCpu = environment->GetCpuStatistics()
            .ValueOrThrow();

        if (validCpuStatistics(jobProxyCpu)) {
            result += jobProxyCpu->SystemUsageTime.value() +
                jobProxyCpu->UserUsageTime.value();
        } else {
            YT_LOG_WARNING("Cannot get cpu statistics from job environment");
        }
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

void TJobProxy::LogSystemStats() const
{
    struct rusage usageSelf, usageChildren;

    getrusage(RUSAGE_SELF, &usageSelf);
    getrusage(RUSAGE_CHILDREN, &usageChildren);

    YT_LOG_INFO("Get processes CPU usage (JobProxyUserCpu: %v, JobProxySystemCpu: %v, "
        "ChildrenUserCpu: %v, ChildrenSystemCpu: %v)",
        TDuration(usageSelf.ru_utime),
        TDuration(usageSelf.ru_stime),
        TDuration(usageChildren.ru_utime),
        TDuration(usageChildren.ru_stime));
}

void TJobProxy::OnProgressSaved(TInstant when)
{
    GetJobOrThrow()->OnProgressSaved(when);
    HeartbeatExecutor_->ScheduleOutOfBand();
}

void TJobProxy::SetOomScoreAdj(int score)
{
    if (OomScoreAdj_.has_value() && *OomScoreAdj_ == score) {
        return;
    }

    pid_t pid = GetPID();

    YT_LOG_DEBUG(
        "Changing oom_score_adj of a job proxy process (Pid: %v, OomScoreAdj: %v)",
        pid,
        score);

    auto config = New<NTools::TChangeOomScoreAdjAsRootConfig>();
    config->Pid = pid;
    config->Score = score;

    try {
        RunTool<NTools::TChangeOomScoreAdjAsRootTool>(config);
        OomScoreAdj_ = score;
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(
            ex,
            "Failed to set oom_score_adj of a job proxy process (Pid: %v)",
            pid);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
