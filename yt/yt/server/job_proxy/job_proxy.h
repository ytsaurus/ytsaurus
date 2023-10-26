#pragma once

#include "public.h"
#include "private.h"
#include "job.h"
#include "environment.h"

#include <yt/yt/server/lib/exec_node/supervisor_service_proxy.h>
#include <yt/yt/library/containers/porto_resource_tracker.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/auth/public.h>

#include <yt/yt/ytlib/job_prober_client/job_probe.h>
#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/library/profiling/solomon/exporter.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/statistics.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobProxy
    : public IJobHost
    , public NJobProberClient::IJobProbe
{
public:
    TJobProxy(
        TJobProxyConfigPtr config,
        NJobTrackerClient::TOperationId operationId,
        NJobTrackerClient::TJobId jobId);

    //! Runs the job. Blocks until the job is complete.
    void Run();

    IInvokerPtr GetControlInvoker() const override;

    std::vector<NChunkClient::TChunkId> DumpInputContext() override;
    TString GetStderr() override;
    NApi::TPollJobShellResponse PollJobShell(
        const NJobProberClient::TJobShellDescriptor& jobShellDescriptor,
        const NYson::TYsonString& parameters) override;
    void Interrupt() override;
    void Fail() override;
    TSharedRef DumpSensors() override;

    NJobTrackerClient::TJobId GetJobId() const override;

    TString GetAuthenticatedUser() const override;

    TString GetLocalHostName() const override;

    NRpc::IServerPtr GetRpcServer() const override;

    TString GetPreparationPath() const override;
    TString GetSlotPath() const override;
    TString GetJobProxyUnixDomainSocketPath() const override;
    TString AdjustPath(const TString& path) const override;

    NChunkClient::TTrafficMeterPtr GetTrafficMeter() const override;

    NConcurrency::IThroughputThrottlerPtr GetInBandwidthThrottler() const override;
    NConcurrency::IThroughputThrottlerPtr GetOutBandwidthThrottler() const override;
    NConcurrency::IThroughputThrottlerPtr GetOutRpsThrottler() const override;
    NConcurrency::IThroughputThrottlerPtr GetUserJobContainerCreationThrottler() const override;

    NApi::NNative::IConnectionPtr CreateNativeConnection(NApi::NNative::TConnectionCompoundConfigPtr config) override;

    TDuration GetSpentCpuTime() const;

    bool TrySetCpuGuarantee(double cpuShare);

private:
    const TJobProxyConfigPtr Config_;
    const NJobTrackerClient::TOperationId OperationId_;
    const NJobTrackerClient::TJobId JobId_;

    //! Can be forever null if running in non-Porto environment.
    TAtomicIntrusivePtr<IJobProxyEnvironment> JobProxyEnvironment_;

    TCpuMonitorPtr CpuMonitor_;

    NTracing::TTraceContextPtr RootSpan_;

    // Job proxy memory reserve (= memory limit after multiplication by
    // job proxy memory reserve factor) by the scheduler.
    i64 JobProxyMemoryReserve_ = 0;
    // Job proxy peak memory usage.
    std::atomic<i64> JobProxyMaxMemoryUsage_ = {0};
    // If this limit for job proxy memory overcommit is exceeded, the job proxy is terminated.
    std::optional<i64> JobProxyMemoryOvercommitLimit_;

    std::atomic<i64> UserJobCurrentMemoryUsage_ = {0};

    std::atomic<bool> Prepared_ = {false};

    // Job proxy and possibly user job peak memory usage.
    i64 TotalMaxMemoryUsage_ = 0;

    // Memory reserve approved by the node.
    std::atomic<i64> ApprovedMemoryReserve_ = {0};
    std::atomic<i64> RequestedMemoryReserve_ = {0};

    std::atomic<i32> NetworkUsage_ = {0};

    std::atomic<double> CpuGuarantee_ = {0};

    const NConcurrency::TActionQueuePtr JobThread_;
    const NConcurrency::TActionQueuePtr ControlThread_;

    const NLogging::TLogger Logger;

    NNodeTrackerClient::TNodeDescriptor LocalDescriptor_;

    NRpc::IServerPtr RpcServer_;

    NConcurrency::IThreadPoolPtr ApiServiceThreadPool_;

    std::unique_ptr<NExecNode::TSupervisorServiceProxy> SupervisorProxy_;

    NApi::NNative::IClientPtr Client_;

    NConcurrency::TPeriodicExecutorPtr HeartbeatExecutor_;
    NConcurrency::TPeriodicExecutorPtr MemoryWatchdogExecutor_;

    TDuration RefCountedTrackerLogPeriod_;
    TInstant LastRefCountedTrackerLogTime_;
    i64 LastLoggedJobProxyMaxMemoryUsage_ = 0;

    TAtomicIntrusivePtr<IJob> Job_;

    IJobSpecHelperPtr JobSpecHelper_;

    std::vector<int> Ports_;

    NChunkClient::TTrafficMeterPtr TrafficMeter_;

    NConcurrency::IThroughputThrottlerPtr InBandwidthThrottler_;
    NConcurrency::IThroughputThrottlerPtr OutBandwidthThrottler_;
    NConcurrency::IThroughputThrottlerPtr OutRpsThrottler_;
    NConcurrency::IThroughputThrottlerPtr UserJobContainerCreationThrottler_;

    NChunkClient::IBlockCachePtr ReaderBlockCache_;

    NProfiling::TSolomonExporterPtr SolomonExporter_;

    NAuth::ITvmBridgePtr TvmBridge_;

    void SetJob(IJobPtr job);
    IJobPtr FindJob() const;
    IJobPtr GetJobOrThrow();

    void SetJobProxyEnvironment(IJobProxyEnvironmentPtr environment);
    IJobProxyEnvironmentPtr FindJobProxyEnvironment() const;

    void EnableRpcProxyInJobProxy(int rpcProxyWorkerThreadPoolSize);

    void DoRun();
    NControllerAgent::NProto::TJobResult RunJob();

    void SendHeartbeat();
    void OnHeartbeatResponse(const TError& error);

    void LogJobSpec(NControllerAgent::NProto::TJobSpec jobSpec);
    void RetrieveJobSpec();
    void ReportResult(
        const NControllerAgent::NProto::TJobResult& result,
        TInstant startTime,
        TInstant finishTime);

    TStatistics GetEnrichedStatistics() const;

    IJobPtr CreateBuiltinJob();

    void UpdateResourceUsage();

    void OnSpawned();
    void OnArtifactsPrepared();

    // IJobHost implementation.
    TJobProxyConfigPtr GetConfig() const override;
    IUserJobEnvironmentPtr CreateUserJobEnvironment(const TJobSpecEnvironmentOptions& options) const override;
    NJobTrackerClient::TOperationId GetOperationId() const override;

    const IJobSpecHelperPtr& GetJobSpecHelper() const override;

    void SetUserJobMemoryUsage(i64 memoryUsage) override;
    void OnResourcesUpdated(i64 memoryUsage, const TError& error);

    void ReleaseNetwork() override;

    NApi::NNative::IClientPtr GetClient() const override;

    void OnPrepared() override;

    void PrepareArtifact(
        const TString& artifactName,
        const TString& pipePath) override;

    void OnArtifactPreparationFailed(
        const TString& artifactName,
        const TString& artifactPath,
        const TError& error) override;

    void OnJobMemoryThrashing() override;

    NChunkClient::TChunkReaderHostPtr GetChunkReaderHost() const override;

    NChunkClient::IBlockCachePtr GetReaderBlockCache() const override;
    NChunkClient::IBlockCachePtr GetWriterBlockCache() const override;

    const NNodeTrackerClient::TNodeDescriptor& LocalDescriptor() const override;

    NLogging::TLogger GetLogger() const override;

    void CheckMemoryUsage();

    void FillJobResult(NControllerAgent::NProto::TJobResult* jobResult);
    void FillStderrResult(NControllerAgent::NProto::TJobResult* jobResult);

    void Abort(EJobProxyExitCode exitCode);

    void LogSystemStats() const;
};

DEFINE_REFCOUNTED_TYPE(TJobProxy)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
