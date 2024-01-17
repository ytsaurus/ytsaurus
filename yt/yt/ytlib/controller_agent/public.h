#pragma once

#include <yt/yt/ytlib/exec_node/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

using NScheduler::TAllocationId;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TCoreInfo;
class TFileDescriptor;
class TTmpfsVolume;

class TJobMetrics;
class TTreeTaggedJobMetrics;
class TPoolTreeSchedulingTagFilter;
class TPoolTreeSchedulingTagFilters;
class TOperationDescriptor;
class TInitializeOperationResult;
class TPrepareOperationResult;
class TMaterializeOperationResult;
class TReviveOperationResult;
class TCommitOperationResult;

class TJobStatus;
class TJobResultExt;
class TOutputResult;

class TJobSpec;
class TJobSpecExt;
class TUserJobSpec;

class TTableInputSpec;
class TQuerySpec;
class TJobProfilerSpec;

class TPartitionJobSpecExt;

class TControllerAgentDescriptor;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using TCoreInfos = std::vector<NProto::TCoreInfo>;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControllerAgentAlertType,
    (UpdateConfig)
    (UnrecognizedConfigOptions)
    (SnapshotLoadingDisabled)
    (UserJobMonitoringLimited)
    (SnapshotBuildingDisabled)
    (ControllerMemoryOverconsumption)
);

DEFINE_ENUM(EControllerState,
    ((Preparing)(0))
    ((Running)(1))
    ((Failing)(2))
    ((Completed)(3))
    ((Failed)(4))
    ((Aborted)(5))
);

////////////////////////////////////////////////////////////////////////////////

using NVectorHdrf::TJobResources;

using NScheduler::TOperationId;
using NJobTrackerClient::TJobId;
using NScheduler::EAbortReason;
using NScheduler::EInterruptReason;
using NScheduler::EOperationType;
using NJobTrackerClient::EJobType;
using NScheduler::TOperationSpecBasePtr;

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::EJobState;
using NExecNode::EJobPhase;

////////////////////////////////////////////////////////////////////////////////

struct TCompletedJobSummary;

DECLARE_REFCOUNTED_CLASS(TProgressCounter)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScheduleAllocationFailReason,
    ((Unknown)                       ( 0))
    ((OperationNotRunning)           ( 1))
    ((NoPendingJobs)                 ( 2))
    ((NotEnoughChunkLists)           ( 3))
    ((NotEnoughResources)            ( 4))
    ((Timeout)                       ( 5))
    ((EmptyInput)                    ( 6))
    ((NoLocalJobs)                   ( 7))
    ((TaskDelayed)                   ( 8))
    ((NoCandidateTasks)              ( 9))
    ((ResourceOvercommit)            (10))
    ((TaskRefusal)                   (11))
    ((JobSpecThrottling)             (12))
    ((IntermediateChunkLimitExceeded)(13))
    ((DataBalancingViolation)        (14))
    ((UnknownNode)                   (15))
    ((UnknownOperation)              (16))
    ((NoAgentAssigned)               (17))
    ((TentativeTreeDeclined)         (18))
    ((NodeBanned)                    (19))
    ((NodeOffline)                   (20))
    ((ControllerThrottling)          (21))
    ((TentativeSpeculativeForbidden) (22))
    ((OperationIsNotAlive)           (23))
    ((NewJobsForbidden)              (24))
    ((NoPendingProbingJobs)          (25))
);

YT_DEFINE_ERROR_ENUM(
    ((AgentCallFailed)                            (4400))
    ((NoOnlineNodeToScheduleAllocation)           (4410))
    ((MaterializationFailed)                      (4415))
    ((OperationControllerMemoryLimitExceeded)     (4416))
    ((IncarnationMismatch)                        (4417))
    ((AgentDisconnected)                          (4418))
    ((NoSuchJob)                                  (4419))
);

////////////////////////////////////////////////////////////////////////////////

extern const TString SecureVaultEnvPrefix;

YT_DEFINE_STRONG_TYPEDEF(TControllerEpoch, int);

YT_DEFINE_STRONG_TYPEDEF(TIncarnationId, NObjectClient::TTransactionId);

using TAgentId = TString;

struct TControllerAgentDescriptor
{
    std::optional<NNodeTrackerClient::TAddressMap> Addresses;
    TIncarnationId IncarnationId;

    TAgentId AgentId;

    operator bool() const;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELayerAccessMethod,
    ((Unknown)  (1)     ("unknown"))
    ((Local)    (2)     ("local"))
    ((Nbd)      (3)     ("nbd"))
);

DEFINE_ENUM(ELayerFilesystem,
    ((Unknown)      (1)     ("unknown"))
    ((Archive)      (2)     ("archive"))
    ((Ext3)         (3)     ("ext3"))
    ((Ext4)         (4)     ("ext4"))
    ((SquashFS)     (5)     ("squashfs"))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
