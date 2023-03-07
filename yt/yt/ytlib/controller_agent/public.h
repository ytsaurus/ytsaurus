#pragma once

#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

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

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EControllerAgentAlertType,
    (UpdateConfig)
    (UnrecognizedConfigOptions)
    (SnapshotLoadingDisabled)
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

using NScheduler::TOperationId;
using NScheduler::TJobId;
using NScheduler::TJobResources;
using NScheduler::EAbortReason;
using NScheduler::EInterruptReason;
using NScheduler::EOperationType;
using NScheduler::EJobType;
using NScheduler::EJobState;
using NScheduler::EJobPhase;
using NScheduler::TOperationSpecBasePtr;
using NScheduler::EOperationAlertType;

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
);

DEFINE_ENUM(EErrorCode,
    ((AgentCallFailed)             (4400))
    ((NoOnlineNodeToScheduleJob)   (4410))
    ((MaterializationFailed)       (4415))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
