#pragma once

#include <yt/yt/ytlib/scheduler/public.h>

#include <yt/yt/ytlib/job_tracker_client/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

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
using NScheduler::TJobId;
using NScheduler::EAbortReason;
using NScheduler::EInterruptReason;
using NScheduler::EOperationType;
using NScheduler::EJobType;
using NScheduler::TOperationSpecBasePtr;

////////////////////////////////////////////////////////////////////////////////

using NJobTrackerClient::EJobState;
using NJobTrackerClient::EJobPhase;

////////////////////////////////////////////////////////////////////////////////

struct TCompletedJobSummary;

DECLARE_REFCOUNTED_CLASS(TProgressCounter)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EScheduleJobFailReason,
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
    ((NoOnlineNodeToScheduleJob)                  (4410))
    ((MaterializationFailed)                      (4415))
    ((OperationControllerMemoryLimitExceeded)     (4416))
    ((IncarnationMismatch)                        (4417))
    ((AgentDisconnected)                          (4418))
);

////////////////////////////////////////////////////////////////////////////////

extern const TString SecureVaultEnvPrefix;

using TControllerEpoch = int;
using TIncarnationId = TGuid;
using TAgentId = TString;

struct TControllerAgentDescriptor
{
    std::optional<NNodeTrackerClient::TAddressMap> Addresses;
    TIncarnationId IncarnationId;

    TAgentId AgentId;

    operator bool() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TLayerAccessMethod
{
    static constexpr TStringBuf Local = "local";
    static constexpr TStringBuf Nbd = "nbd";
    static constexpr TStringBuf Default = "local";

    static bool IsKnownAccessMethod(const TStringBuf& accessMethod)
    {
        return accessMethod == Local || accessMethod == Nbd;
    }
};

struct TLayerFilesystem
{
    static constexpr TStringBuf Archive = "archive";
    static constexpr TStringBuf Ext3 = "ext3";
    static constexpr TStringBuf Ext4 = "ext4";
    static constexpr TStringBuf SquashFS = "squashfs";
    static constexpr TStringBuf Default = "archive";

    static bool IsKnownFilesystem(const TStringBuf& filesystem)
    {
        return  filesystem == Archive ||
                filesystem == Ext3 ||
                filesystem == Ext4 ||
                filesystem == SquashFS;
    }

    static bool IsCompatible(const TStringBuf& accessMethod, const TStringBuf& filesystem)
    {
        if (accessMethod == TLayerAccessMethod::Nbd && filesystem == Archive) {
            return false;
        }

        if (accessMethod == TLayerAccessMethod::Local && filesystem == Ext3) {
            return false;
        }

        if (accessMethod == TLayerAccessMethod::Local && filesystem == Ext4) {
            return false;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
