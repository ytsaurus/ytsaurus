#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/library/containers/config.h>

#include <yt/yt/library/dns_over_rpc/client/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/ytree/node.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class TJobThrashingDetectorConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enabled;

    TDuration CheckPeriod;

    int MajorPageFaultCountLimit;

    // Job will be aborted upon violating MajorPageFaultCountLimit this number of times in a row.
    int LimitOverflowCountThresholdToAbortJob;

    REGISTER_YSON_STRUCT(TJobThrashingDetectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobThrashingDetectorConfig)

//! Describes configuration of a single environment.
class TJobEnvironmentConfig
    : public virtual NYTree::TYsonStruct
{
public:
    EJobEnvironmentType Type;

    //! When job control is enabled, system runs user jobs under fake
    //! uids in range [StartUid, StartUid + SlotCount - 1].
    int StartUid;

    TDuration MemoryWatchdogPeriod;

    TJobThrashingDetectorConfigPtr JobThrashingDetector;

    REGISTER_YSON_STRUCT(TJobEnvironmentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobEnvironmentConfig
    : public TJobEnvironmentConfig
{
    REGISTER_YSON_STRUCT(TSimpleJobEnvironmentConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TSimpleJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestingJobEnvironmentScenario,
    (None)
    (IncreasingMajorPageFaultCount)
)

////////////////////////////////////////////////////////////////////////////////

class TTestingJobEnvironmentConfig
    : public TSimpleJobEnvironmentConfig
{
public:
    ETestingJobEnvironmentScenario TestingJobEnvironmentScenario;

    REGISTER_YSON_STRUCT(TTestingJobEnvironmentConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TTestingJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TPortoJobEnvironmentConfig
    : public TJobEnvironmentConfig
{
public:
    NContainers::TPortoExecutorDynamicConfigPtr PortoExecutor;

    TDuration BlockIOWatchdogPeriod;

    THashMap<TString, TString> ExternalBinds;

    double JobsIOWeight;
    double NodeDedicatedCpu;

    bool UseShortContainerNames;

    // COMPAT(psushin): this is compatibility option between different versions of ytcfgen and yt_node.
    //! Used by ytcfgen, when it creates "yt_daemon" subcontainer inside iss_hook_start.
    bool UseDaemonSubcontainer;

    //! For testing purposes only.
    bool UseExecFromLayer;

    //! Allow mounting /dev/fuse to user job conatiners.
    bool AllowMountFuseDevice;

    //! Backoff time between container destruction attempts.
    TDuration ContainerDestructionBackoff;

    REGISTER_YSON_STRUCT(TPortoJobEnvironmentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPortoJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotLocationConfig
    : public TDiskLocationConfig
{
public:
    std::optional<i64> DiskQuota;
    i64 DiskUsageWatermark;

    TString MediumName;

    REGISTER_YSON_STRUCT(TSlotLocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotLocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TNumaNodeConfig
    : public virtual NYTree::TYsonStruct
{
public:
    i64 NumaNodeId;
    i64 CpuCount;
    TString CpuSet;

    REGISTER_YSON_STRUCT(TNumaNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNumaNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotManagerTestingConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! If set, slot manager does not report JobProxyUnavailableAlert
    //! allowing scheduler to schedule jobs to current node. Such jobs are
    //! going to be aborted instead of failing; that is exactly what we test
    //! using this switch.
    bool SkipJobProxyUnavailableAlert;

    REGISTER_YSON_STRUCT(TSlotManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerTestingConfig)

class TSlotManagerConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! Root path for slot directories.
    std::vector<TSlotLocationConfigPtr> Locations;

    //! Enable using tmpfs on the node.
    bool EnableTmpfs;

    //! Use MNT_DETACH when tmpfs umount called. When option enabled the "Device is busy" error is impossible,
    //! because actual umount will be performed by Linux core asynchronously.
    bool DetachedTmpfsUmount;

    //! Polymorphic job environment configuration.
    NYTree::INodePtr JobEnvironment;

    bool EnableReadWriteCopy;

    //! Chunk size used for copying chunks if #copy_chunks is set to %true in operation spec.
    i64 FileCopyChunkSize;

    TDuration DiskResourcesUpdatePeriod;

    TDuration SlotLocationStatisticsUpdatePeriod;

    int MaxConsecutiveJobAborts;

    int MaxConsecutiveGpuJobFailures;

    TDuration DisableJobsTimeout;

    //! Default medium used to run jobs without disk requests.
    TString DefaultMediumName;

    bool DisableJobsOnGpuCheckFailure;

    double IdleCpuFraction;

    TSlotManagerTestingConfigPtr Testing;

    std::vector<TNumaNodeConfigPtr> NumaNodes;

    REGISTER_YSON_STRUCT(TSlotManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class THeartbeatReporterDynamicConfigBase
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Random delay before first heartbeat.
    std::optional<TDuration> HeartbeatSplay;

    //! Start backoff for sending the next heartbeat after a failure.
    std::optional<TDuration> FailedHeartbeatBackoffStartTime;

    //! Maximum backoff for sending the next heartbeat after a failure.
    std::optional<TDuration> FailedHeartbeatBackoffMaxTime;

    //! Backoff mulitplier for sending the next heartbeat after a failure.
    std::optional<double> FailedHeartbeatBackoffMultiplier;

    REGISTER_YSON_STRUCT(THeartbeatReporterDynamicConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
public:
    bool SendHeartbeatOnJobFinished;

    REGISTER_YSON_STRUCT(TSchedulerConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
public:
    TDuration TestHeartbeatDelay;
    NConcurrency::TThroughputThrottlerConfigPtr StatisticsThrottler;
    std::optional<TDuration> RunningJobStatisticsSendingBackoff;
    bool SendWaitingJobs;
    TDuration TotalConfirmationPeriod;

    REGISTER_YSON_STRUCT(TControllerAgentConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class THeartbeatReporterConfigBase
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent heartbeats.
    TDuration HeartbeatPeriod;

    //! Random delay before first heartbeat.
    TDuration HeartbeatSplay;

    //! Start backoff for sending the next heartbeat after a failure.
    TDuration FailedHeartbeatBackoffStartTime;

    //! Maximum backoff for sending the next heartbeat after a failure.
    TDuration FailedHeartbeatBackoffMaxTime;

    //! Backoff mulitplier for sending the next heartbeat after a failure.
    double FailedHeartbeatBackoffMultiplier;

    void ApplyDynamicInplace(const THeartbeatReporterDynamicConfigBase& dynamicConfig);

    REGISTER_YSON_STRUCT(THeartbeatReporterConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TSchedulerConnectorConfig
    : public THeartbeatReporterConfigBase
{
public:
    TSchedulerConnectorConfigPtr ApplyDynamic(const TSchedulerConnectorDynamicConfigPtr& dynamicConfig);

    void ApplyDynamicInplace(const TSchedulerConnectorDynamicConfig& dynamicConfig);

    REGISTER_YSON_STRUCT(TSchedulerConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSchedulerConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnectorConfig
    : public THeartbeatReporterConfigBase
{
public:
    NConcurrency::TThroughputThrottlerConfigPtr StatisticsThrottler;
    TDuration RunningJobStatisticsSendingBackoff;

    TControllerAgentConnectorConfigPtr ApplyDynamic(const TControllerAgentConnectorDynamicConfigPtr& dynamicConfig);
    void ApplyDynamicInplace(const TControllerAgentConnectorDynamicConfig& dynamicConfig);

    REGISTER_YSON_STRUCT(TControllerAgentConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent exec node heartbeats.
    TDuration HeartbeatPeriod;

    //! Splay for exec node heartbeats.
    TDuration HeartbeatPeriodSplay;

    REGISTER_YSON_STRUCT(TMasterConnectorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserJobSensor
    : public NYTree::TYsonStruct
{
public:
    NProfiling::EMetricType Type;
    EUserJobSensorSource Source;
    // Path in statistics structure.
    std::optional<TString> Path;
    TString ProfilingName;

    REGISTER_YSON_STRUCT(TUserJobSensor);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobSensor)

////////////////////////////////////////////////////////////////////////////////

class TUserJobMonitoringConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TUserJobSensorPtr> Sensors;

    static const THashMap<TString, TUserJobSensorPtr>& GetDefaultSensors();

    REGISTER_YSON_STRUCT(TUserJobMonitoringConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobMonitoringConfig)

////////////////////////////////////////////////////////////////////////////////

class TUserJobMonitoringDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TUserJobSensorPtr> Sensors;

    REGISTER_YSON_STRUCT(TUserJobMonitoringDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobMonitoringDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecNodeConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TSlotManagerConfigPtr SlotManager;
    NJobAgent::TJobControllerConfigPtr JobController;
    TJobReporterConfigPtr JobReporter;
    TControllerAgentConnectorConfigPtr ControllerAgentConnector;
    TSchedulerConnectorConfigPtr SchedulerConnector;

    NLogging::TLogManagerConfigPtr JobProxyLogging;
    NTracing::TJaegerTracerConfigPtr JobProxyJaeger;
    std::optional<TString> JobProxyStderrPath;

    TDuration SupervisorRpcTimeout;
    TDuration JobProberRpcTimeout;

    TDuration JobProxyHeartbeatPeriod;

    bool JobProxyUploadDebugArtifactChunks;

    bool JobProxySendHeartbeatBeforeAbort;

    NDns::TDnsOverRpcResolverConfigPtr JobProxyDnsOverRpcResolver;

    //! This is a special testing option.
    //! Instead of actually setting root fs, it just provides special environment variable.
    bool TestRootFS;
    bool EnableArtifactCopyTracking;
    bool UseCommonRootFsQuota;
    bool UseArtifactBinds;

    std::vector<NJobProxy::TBindConfigPtr> RootFSBinds;

    int NodeDirectoryPrepareRetryCount;
    TDuration NodeDirectoryPrepareBackoffTime;

    TDuration JobProxyPreparationTimeout;

    i64 MinRequiredDiskSpace;

    TDuration JobAbortionTimeout;

    std::optional<TDuration> JobPrepareTimeLimit;

    //! This option is used for testing purposes only.
    //! Adds inner errors for failed jobs.
    bool TestJobErrorTruncation;

    NJobProxy::TCoreWatcherConfigPtr CoreWatcher;

    //! This option is used for testing purposes only.
    //! It runs job shell under root user instead of slot user.
    bool TestPollJobShell;

    //! If set, user job will not receive uid.
    //! For testing purposes only.
    bool DoNotSetUserId;

    NConcurrency::TThroughputThrottlerConfigPtr UserJobContainerCreationThrottler;

    TDuration MemoryTrackerCachePeriod;
    TDuration SMapsMemoryTrackerCachePeriod;

    TUserJobMonitoringConfigPtr UserJobMonitoring;

    TMasterConnectorConfigPtr MasterConnector;

    NProfiling::TSolomonExporterConfigPtr JobProxySolomonExporter;
    TDuration SensorDumpTimeout;

    //! This option can disable memory limit check for user jobs.
    //! Used in arcadia tests, since it's almost impossible to set
    //! proper memory limits for asan builds.
    bool CheckUserJobMemoryLimit;

    //! Enables job abort on violated memory reserve.
    bool AlwaysAbortOnMemoryReserveOverdraft;

    REGISTER_YSON_STRUCT(TExecNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Period between consequent exec node heartbeats.
    std::optional<TDuration> HeartbeatPeriod;

    //! Splay for exec node heartbeats.
    std::optional<TDuration> HeartbeatPeriodSplay;

    //! Timeout of the exec node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<bool> DisableJobsOnGpuCheckFailure;

    //! Enables disk usage checks in periodic disk resources update.
    bool CheckDiskSpaceLimit;

    //! How to distribute cpu resources between 'common' and 'idle' slots.
    std::optional<double> IdleCpuFraction;

    bool EnableNumaNodeScheduling;

    bool EnableJobEnvironmentResurrection;

    REGISTER_YSON_STRUCT(TSlotManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TVolumeManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableAsyncLayerRemoval;

    //! For testing.
    std::optional<TDuration> DelayAfterLayerImported;

    REGISTER_YSON_STRUCT(TVolumeManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TVolumeManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecNodeDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TMasterConnectorDynamicConfigPtr MasterConnector;

    TSlotManagerDynamicConfigPtr SlotManager;

    TVolumeManagerDynamicConfigPtr VolumeManager;

    NJobAgent::TJobControllerDynamicConfigPtr JobController;

    TJobReporterDynamicConfigPtr JobReporter;

    TSchedulerConnectorDynamicConfigPtr SchedulerConnector;
    TControllerAgentConnectorDynamicConfigPtr ControllerAgentConnector;

    std::optional<TDuration> JobAbortionTimeout;
    TDuration SlotReleaseTimeout;

    std::optional<TDuration> JobProxyPreparationTimeout;

    bool AbortOnJobsDisabled;

    bool TreatJobProxyFailureAsAbort;

    TUserJobMonitoringDynamicConfigPtr UserJobMonitoring;

    //! Job throttler config, eg. its RPC timeout and backoff.
    NJobProxy::TJobThrottlerConfigPtr JobThrottler;

    NConcurrency::TThroughputThrottlerConfigPtr UserJobContainerCreationThrottler;

    std::optional<int> StatisticsOutputTableCountLimit;

    REGISTER_YSON_STRUCT(TExecNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
