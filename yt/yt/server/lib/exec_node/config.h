#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/library/containers/config.h>

#include <yt/yt/library/containers/cri/config.h>

#include <yt/yt/library/dns_over_rpc/client/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/nbd/config.h>

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

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSimpleJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETestingJobEnvironmentScenario,
    (None)
    (IncreasingMajorPageFaultCount)
);

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

    //! Allow mounting /dev/fuse to user job containers.
    bool AllowMountFuseDevice;

    //! Backoff time between container destruction attempts.
    TDuration ContainerDestructionBackoff;

    REGISTER_YSON_STRUCT(TPortoJobEnvironmentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPortoJobEnvironmentConfig)

////////////////////////////////////////////////////////////////////////////////

class TCriJobEnvironmentConfig
    : public TJobEnvironmentConfig
{
public:
    NContainers::NCri::TCriExecutorConfigPtr CriExecutor;

    TString JobProxyImage;

    //! Bind mounts for job proxy container.
    //! For now works as "root_fs_binds" because user job runs in the same container.
    std::vector<NJobProxy::TBindConfigPtr> JobProxyBindMounts;

    //! Do not bind mount jobproxy binary into container
    bool UseJobProxyFromImage;

    REGISTER_YSON_STRUCT(TCriJobEnvironmentConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCriJobEnvironmentConfig)

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

    int MaxConsecutiveGpuJobFailures;

    //! Default medium used to run jobs without disk requests.
    TString DefaultMediumName;

    TSlotManagerTestingConfigPtr Testing;

    std::vector<TNumaNodeConfigPtr> NumaNodes;

    REGISTER_YSON_STRUCT(TSlotManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TSlotManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TSlotManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    bool DisableJobsOnGpuCheckFailure;

    //! Enables disk usage checks in periodic disk resources update.
    bool CheckDiskSpaceLimit;

    //! How to distribute cpu resources between 'common' and 'idle' slots.
    double IdleCpuFraction;

    bool EnableNumaNodeScheduling;

    bool EnableJobEnvironmentResurrection;

    int MaxConsecutiveJobAborts;

    TDuration DisableJobsTimeout;

    //! Polymorphic job environment configuration.
    NYTree::INodePtr JobEnvironment;

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

class THeartbeatReporterDynamicConfigBase
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

    //! Backoff multiplier for sending the next heartbeat after a failure.
    double FailedHeartbeatBackoffMultiplier;

    REGISTER_YSON_STRUCT(THeartbeatReporterDynamicConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
public:
    TDuration SettleJobsTimeout;

    TDuration TestHeartbeatDelay;

    NConcurrency::TThroughputThrottlerConfigPtr StatisticsThrottler;
    TDuration RunningJobStatisticsSendingBackoff;

    TDuration TotalConfirmationPeriod;

    bool UseJobTrackerServiceToSettleJobs;

    REGISTER_YSON_STRUCT(TControllerAgentConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TControllerAgentConnectorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
public:
    //! Timeout of the exec node heartbeat RPC request.
    TDuration HeartbeatTimeout;

    REGISTER_YSON_STRUCT(TMasterConnectorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConnectorDynamicConfig)

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

DEFINE_ENUM(EGpuInfoSourceType,
    (NvGpuManager)
    (NvidiaSmi)
);

////////////////////////////////////////////////////////////////////////////////

class TGpuInfoSourceConfig
    : public NYTree::TYsonStruct
{
public:
    EGpuInfoSourceType Type;
    TString NvGpuManagerServiceAddress;
    TString NvGpuManagerServiceName;
    std::optional<TString> NvGpuManagerDevicesCgroupPath;
    bool GpuIndexesFromNvidiaSmi;

    REGISTER_YSON_STRUCT(TGpuInfoSourceConfig);
    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuInfoSourceConfig)

////////////////////////////////////////////////////////////////////////////////

class TGpuManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    TDuration HealthCheckTimeout;
    TDuration HealthCheckPeriod;

    TDuration HealthCheckFailureBackoff;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;

    std::optional<NYPath::TYPath> DriverLayerDirectoryPath;
    std::optional<TString> DriverVersion;
    TDuration DriverLayerFetchPeriod;
    TDuration DriverLayerFetchPeriodSplay;

    THashMap<TString, TString> CudaToolkitMinDriverVersion;

    TGpuInfoSourceConfigPtr GpuInfoSource;

    // TODO(eshcherbin): Extract test options to subconfig?
    //! This is a special testing option.
    //! Instead of normal gpu discovery, it forces the node to believe the number of GPUs passed in the config.
    bool TestResource;
    //! These options enable testing gpu layers and setup commands.
    bool TestLayers;
    bool TestSetupCommands;
    bool TestExtraGpuCheckCommandFailure;
    int TestGpuCount;
    double TestUtilizationGpuRate;
    TDuration TestGpuInfoUpdatePeriod;

    REGISTER_YSON_STRUCT(TGpuManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TGpuManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TDuration> HealthCheckTimeout;
    std::optional<TDuration> HealthCheckPeriod;
    std::optional<TDuration> HealthCheckFailureBackoff;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;

    std::optional<TDuration> DriverLayerFetchPeriod;

    std::optional<THashMap<TString, TString>> CudaToolkitMinDriverVersion;

    TGpuInfoSourceConfigPtr GpuInfoSource;

    REGISTER_YSON_STRUCT(TGpuManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TShellCommandConfig
    : public NYTree::TYsonStruct
{
public:
    TString Path;
    std::vector<TString> Args;

    REGISTER_YSON_STRUCT(TShellCommandConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TShellCommandConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobControllerConfig
    : public NYTree::TYsonStruct
{
public:
    NJobAgent::TResourceLimitsConfigPtr ResourceLimits;
    TDuration WaitingJobsTimeout;

    TDuration CpuOverdraftTimeout;
    TDuration MemoryOverdraftTimeout;

    TDuration ProfilingPeriod;

    TDuration ResourceAdjustmentPeriod;

    TDuration RecentlyRemovedJobsCleanPeriod;
    TDuration RecentlyRemovedJobsStoreTimeout;

    i64 FreeMemoryWatermark;

    double CpuPerTabletSlot;

    std::optional<double> CpuToVCpuFactor;
    std::optional<TString> CpuModel;

    //! Port set has higher priority than StartPort ans PortCount if it is specified.
    int StartPort;
    int PortCount;
    std::optional<THashSet<int>> PortSet;

    TGpuManagerConfigPtr GpuManager;

    NJobAgent::TMappedMemoryControllerConfigPtr MappedMemoryController;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;
    TString SetupCommandUser;

    TDuration JobProxyBuildInfoUpdatePeriod;

    bool DisableJobProxyProfiling;

    TDuration UnknownOperationJobsRemovalDelay;

    REGISTER_YSON_STRUCT(TJobControllerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobControllerConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobControllerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TDuration> CpuOverdraftTimeout;
    std::optional<double> CpuToVCpuFactor;
    bool EnableCpuToVCpuFactor;
    bool AccountMasterMemoryRequest;

    std::optional<THashMap<TString, double>> CpuModelToCpuToVCpuFactor;
    std::optional<TDuration> MemoryOverdraftTimeout;

    std::optional<TDuration> ProfilingPeriod;

    std::optional<TDuration> ResourceAdjustmentPeriod;

    std::optional<TDuration> RecentlyRemovedJobsCleanPeriod;
    std::optional<TDuration> RecentlyRemovedJobsStoreTimeout;

    std::optional<TDuration> JobProxyBuildInfoUpdatePeriod;

    std::optional<bool> DisableJobProxyProfiling;

    TGpuManagerDynamicConfigPtr GpuManager;

    NJobProxy::TJobProxyDynamicConfigPtr JobProxy;

    NJobAgent::TMemoryPressureDetectorConfigPtr MemoryPressureDetector;

    TDuration OperationInfosRequestPeriod;

    std::optional<TDuration> UnknownOperationJobsRemovalDelay;

    TDuration DisabledJobsInterruptionTimeout;

    REGISTER_YSON_STRUCT(TJobControllerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobControllerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TNbdClientConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TDuration Timeout;

    REGISTER_YSON_STRUCT(TNbdClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdClientConfig)

////////////////////////////////////////////////////////////////////////////////

class TNbdConfig
    : public virtual NYTree::TYsonStruct
{
public:
    bool Enabled;
    i64 BlockCacheCompressedDataCapacity;
    TNbdClientConfigPtr Client;
    NNbd::TNbdServerConfigPtr Server;

    REGISTER_YSON_STRUCT(TNbdConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecNodeConfig
    : public virtual NYTree::TYsonStruct
{
public:
    TSlotManagerConfigPtr SlotManager;
    TJobControllerConfigPtr JobController;
    TJobReporterConfigPtr JobReporter;

    NLogging::TLogManagerConfigPtr JobProxyLogging;
    NTracing::TJaegerTracerConfigPtr JobProxyJaeger;
    std::optional<TString> JobProxyStderrPath;
    std::optional<TString> ExecutorStderrPath;

    TDuration SupervisorRpcTimeout;
    TDuration JobProberRpcTimeout;

    TDuration JobProxyHeartbeatPeriod;

    bool JobProxySendHeartbeatBeforeAbort;

    NDns::TDnsOverRpcResolverConfigPtr JobProxyDnsOverRpcResolver;

    //! This is a special testing option.
    //! Instead of actually setting root fs, it just provides special environment variable.
    bool TestRootFS;
    bool EnableArtifactCopyTracking;
    bool UseCommonRootFSQuota;
    bool UseArtifactBinds;

    //! Bind mounts added for all user job containers.
    //! Should include ChunkCache if artifacts are passed by symlinks.
    std::vector<NJobProxy::TBindConfigPtr> RootFSBinds;

    int NodeDirectoryPrepareRetryCount;
    TDuration NodeDirectoryPrepareBackoffTime;

    TDuration JobProxyPreparationTimeout;

    i64 MinRequiredDiskSpace;

    TDuration WaitingForJobCleanupTimeout;

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

    NAuth::TAuthenticationManagerConfigPtr JobProxyAuthenticationManager;

    NProfiling::TSolomonExporterConfigPtr JobProxySolomonExporter;
    TDuration SensorDumpTimeout;

    //! This option can disable memory limit check for user jobs.
    //! Used in arcadia tests, since it's almost impossible to set
    //! proper memory limits for asan builds.
    bool CheckUserJobMemoryLimit;

    //! Enables job abort on violated memory reserve.
    bool AlwaysAbortOnMemoryReserveOverdraft;

    TNbdConfigPtr Nbd;

    REGISTER_YSON_STRUCT(TExecNodeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecNodeDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TMasterConnectorDynamicConfigPtr MasterConnector;

    TSlotManagerDynamicConfigPtr SlotManager;

    TVolumeManagerDynamicConfigPtr VolumeManager;

    TJobControllerDynamicConfigPtr JobController;

    TJobReporterDynamicConfigPtr JobReporter;

    TSchedulerConnectorDynamicConfigPtr SchedulerConnector;
    TControllerAgentConnectorDynamicConfigPtr ControllerAgentConnector;

    std::optional<TDuration> WaitingForJobCleanupTimeout;
    TDuration SlotReleaseTimeout;

    bool AbortOnFreeVolumeSynchronizationFailed;

    bool AbortOnFreeSlotSynchronizationFailed;

    std::optional<TDuration> JobProxyPreparationTimeout;

    bool AbortOnJobsDisabled;

    bool AbortOnOperationWithVolumeFailed;

    bool AbortOnOperationWithLayerFailed;

    bool TreatJobProxyFailureAsAbort;

    TUserJobMonitoringDynamicConfigPtr UserJobMonitoring;

    //! Job throttler config, eg. its RPC timeout and backoff.
    NJobProxy::TJobThrottlerConfigPtr JobThrottler;

    NConcurrency::TThroughputThrottlerConfigPtr UserJobContainerCreationThrottler;

    std::optional<int> StatisticsOutputTableCountLimit;

    // NB(yuryalekseev): At the moment dynamic NBD config is used only to create
    // NBD server during startup or to dynamically enable/disable creation of NBD volumes.
    TNbdConfigPtr Nbd;

    REGISTER_YSON_STRUCT(TExecNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
