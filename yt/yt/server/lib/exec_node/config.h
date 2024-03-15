#pragma once

#include "public.h"

#include <yt/yt/server/lib/job_agent/config.h>

#include <yt/yt/server/lib/job_proxy/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/nbd/config.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/containers/config.h>

#include <yt/yt/library/containers/cri/config.h>

#include <yt/yt/library/dns_over_rpc/client/config.h>

#include <yt/yt/library/gpu/config.h>

#include <yt/yt/core/concurrency/config.h>

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

    bool EnableArtifactCopyTracking;

    //! If set, user job will not receive uid.
    //! For testing purposes only.
    bool DoNotSetUserId;

    //! Chunk size used for copying chunks if #copy_chunks is set to %true in operation spec.
    i64 FileCopyChunkSize;

    TDuration DiskResourcesUpdatePeriod;

    TDuration SlotLocationStatisticsUpdatePeriod;

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

    int MaxConsecutiveGpuJobFailures;

    int MaxConsecutiveJobAborts;

    TConstantBackoffOptions DisableJobsBackoffStrategy;

    // COMPAT(psushin): temporary flag to disable CloseAllDescriptors machinery.
    bool ShouldCloseDescriptors;

    TDuration SlotReleaseTimeout;

    bool AbortOnFreeVolumeSynchronizationFailed;

    bool AbortOnFreeSlotSynchronizationFailed;

    bool AbortOnJobsDisabled;

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
    //! For testing.
    std::optional<TDuration> DelayAfterLayerImported;

    bool EnableAsyncLayerRemoval;

    bool AbortOnOperationWithVolumeFailed;

    bool AbortOnOperationWithLayerFailed;

    //! For testing purpuses.
    bool ThrowOnPrepareVolume;

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

class TUserJobMonitoringDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    THashMap<TString, TUserJobSensorPtr> Sensors;

    static const THashMap<TString, TUserJobSensorPtr>& GetDefaultSensors();

    REGISTER_YSON_STRUCT(TUserJobMonitoringDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUserJobMonitoringDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class THeartbeatReporterDynamicConfigBase
    : public NYTree::TYsonStruct
{
public:
    NConcurrency::TRetryingPeriodicExecutorOptions HeartbeatExecutor;

    REGISTER_YSON_STRUCT(THeartbeatReporterDynamicConfigBase);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TControllerAgentConnectorDynamicConfig
    : public THeartbeatReporterDynamicConfigBase
{
public:
    TDuration JobStalenessDelay;

    TDuration SettleJobsTimeout;

    TDuration TestHeartbeatDelay;

    NConcurrency::TThroughputThrottlerConfigPtr StatisticsThrottler;
    TDuration RunningJobStatisticsSendingBackoff;

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

class TGpuManagerTestingConfig
    : public NYTree::TYsonStruct
{
public:
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

    REGISTER_YSON_STRUCT(TGpuManagerTestingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerTestingConfig);

////////////////////////////////////////////////////////////////////////////////

class TGpuManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;

    std::optional<NYPath::TYPath> DriverLayerDirectoryPath;
    std::optional<TString> DriverVersion;

    NGpu::TGpuInfoSourceConfigPtr GpuInfoSource;

    TGpuManagerTestingConfigPtr Testing;

    REGISTER_YSON_STRUCT(TGpuManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TGpuManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration HealthCheckTimeout;
    TDuration HealthCheckPeriod;
    TDuration HealthCheckFailureBackoff;

    TDuration RdmaDeviceInfoUpdateTimeout;
    TDuration RdmaDeviceInfoUpdatePeriod;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;

    NConcurrency::TPeriodicExecutorOptions DriverLayerFetching;

    THashMap<TString, TString> CudaToolkitMinDriverVersion;

    NGpu::TGpuInfoSourceConfigPtr GpuInfoSource;

    //! This option is specific to nvidia-container-runtime.
    TString DefaultNvidiaDriverCapabilities;

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

class TJobCommonConfig
    : public NYTree::TYsonStruct
{
public:
    bool UseArtifactBinds;

    int NodeDirectoryPrepareRetryCount;

    TDuration NodeDirectoryPrepareBackoffTime;

    TDuration JobProxyPreparationTimeout;

    TDuration WaitingForJobCleanupTimeout;

    std::optional<TDuration> JobPrepareTimeLimit;

    //! This option is used for testing purposes only.
    //! Adds inner errors for failed jobs.
    bool TestJobErrorTruncation;

    TDuration MemoryTrackerCachePeriod;

    TDuration SMapsMemoryTrackerCachePeriod;

    TUserJobMonitoringDynamicConfigPtr UserJobMonitoring;

    TDuration SensorDumpTimeout;

    bool TreatJobProxyFailureAsAbort;

    std::optional<TShellCommandConfigPtr> JobSetupCommand;
    TString SetupCommandUser;

    std::optional<int> StatisticsOutputTableCountLimit;

    //! Job throttler config, eg. its RPC timeout and backoff.
    NJobProxy::TJobThrottlerConfigPtr JobThrottler;

    REGISTER_YSON_STRUCT(TJobCommonConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobCommonConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobControllerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TConstantBackoffOptions OperationInfoRequestBackoffStrategy;

    TDuration WaitingJobsTimeout;

    TDuration CpuOverdraftTimeout;

    i64 MinRequiredDiskSpace;

    TDuration MemoryOverdraftTimeout;

    TDuration ResourceAdjustmentPeriod;

    TDuration RecentlyRemovedJobsCleanPeriod;
    TDuration RecentlyRemovedJobsStoreTimeout;

    TDuration JobProxyBuildInfoUpdatePeriod;

    bool DisableJobProxyProfiling;

    NJobProxy::TJobProxyDynamicConfigPtr JobProxy;

    TDuration UnknownOperationJobsRemovalDelay;

    TDuration DisabledJobsInterruptionTimeout;

    TJobCommonConfigPtr JobCommon;

    TDuration ProfilingPeriod;

    REGISTER_YSON_STRUCT(TJobControllerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobControllerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TNbdClientConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration IOTimeout;
    TDuration ReconnectTimeout;
    int ConnectionCount;

    REGISTER_YSON_STRUCT(TNbdClientConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdClientConfig)

////////////////////////////////////////////////////////////////////////////////

class TNbdConfig
    : public NYTree::TYsonStruct
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

class TJobProxyConfig
    : public NYTree::TYsonStruct
{
public:
    NLogging::TLogManagerConfigPtr JobProxyLogging;

    NTracing::TJaegerTracerConfigPtr JobProxyJaeger;

    NDns::TDnsOverRpcResolverConfigPtr JobProxyDnsOverRpcResolver;

    NAuth::TAuthenticationManagerConfigPtr JobProxyAuthenticationManager;

    NJobProxy::TCoreWatcherConfigPtr CoreWatcher;

    std::optional<TString> JobProxyStderrPath;
    std::optional<TString> ExecutorStderrPath;

    TDuration SupervisorRpcTimeout;

    TDuration JobProxyHeartbeatPeriod;

    bool JobProxySendHeartbeatBeforeAbort;

    //! This is a special testing option.
    //! Instead of actually setting root fs, it just provides special environment variable.
    bool TestRootFS;

    //! This option is used for testing purposes only.
    //! It runs job shell under root user instead of slot user.
    bool TestPollJobShell;

    //! This option can disable memory limit check for user jobs.
    //! Used in arcadia tests, since it's almost impossible to set
    //! proper memory limits for asan builds.
    bool CheckUserJobMemoryLimit;

    //! Enables job abort on violated memory reserve.
    bool AlwaysAbortOnMemoryReserveOverdraft;

    REGISTER_YSON_STRUCT(TJobProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TExecNodeConfig
    : public NYTree::TYsonStruct
{
public:
    //! Bind mounts added for all user job containers.
    //! Should include ChunkCache if artifacts are passed by symlinks.
    std::vector<NJobProxy::TBindConfigPtr> RootFSBinds;

    TSlotManagerConfigPtr SlotManager;

    TGpuManagerConfigPtr GpuManager;

    NProfiling::TSolomonExporterConfigPtr JobProxySolomonExporter;

    TJobProxyConfigPtr JobProxy;

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

    TGpuManagerDynamicConfigPtr GpuManager;

    TJobControllerDynamicConfigPtr JobController;

    TJobReporterConfigPtr JobReporter;

    TSchedulerConnectorDynamicConfigPtr SchedulerConnector;

    TControllerAgentConnectorDynamicConfigPtr ControllerAgentConnector;

    NConcurrency::TThroughputThrottlerConfigPtr UserJobContainerCreationThrottler;

    // NB(yuryalekseev): At the moment dynamic NBD config is used only to create
    // NBD server during startup or to dynamically enable/disable creation of NBD volumes.
    TNbdConfigPtr Nbd;

    REGISTER_YSON_STRUCT(TExecNodeDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TExecNodeDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
