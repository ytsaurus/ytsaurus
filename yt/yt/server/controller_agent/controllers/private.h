#pragma once

#include <yt/yt/server/controller_agent/private.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EResourceOverdraftStatus,
    (None)
    (Once)
    (MultipleTimes)
);

DEFINE_ENUM(EPredecessorType,
    (None)
    (ResourceOverdraft)
);

class TOperationControllerBase;

DECLARE_REFCOUNTED_STRUCT(TJoblet)
DECLARE_REFCOUNTED_STRUCT(TCompletedJob)

DECLARE_REFCOUNTED_CLASS(TTask)

DECLARE_REFCOUNTED_CLASS(TAutoMergeTask)

DECLARE_REFCOUNTED_STRUCT(ITaskHost)

DECLARE_REFCOUNTED_STRUCT(IAlertManager)
DECLARE_REFCOUNTED_STRUCT(IAlertManagerHost)

DECLARE_REFCOUNTED_CLASS(TDataBalancer)

DECLARE_REFCOUNTED_STRUCT(TInputTable)
DECLARE_REFCOUNTED_STRUCT(TOutputTable)
DECLARE_REFCOUNTED_STRUCT(TIntermediateTable)

DECLARE_REFCOUNTED_CLASS(TClusterResolver)
DECLARE_REFCOUNTED_CLASS(TInputTransactionManager)
DECLARE_REFCOUNTED_CLASS(TInputCluster)
DECLARE_REFCOUNTED_CLASS(TInputManager)
DECLARE_REFCOUNTED_STRUCT(IInputManagerHost)
DECLARE_REFCOUNTED_CLASS(TUnavailableChunksWatcher)
DECLARE_REFCOUNTED_CLASS(TCombiningSamplesFetcher)

DECLARE_REFCOUNTED_CLASS(TDataFlowGraph)
DECLARE_REFCOUNTED_CLASS(TLivePreview)
DECLARE_REFCOUNTED_STRUCT(TInputStreamDescriptor)
DECLARE_REFCOUNTED_STRUCT(TOutputStreamDescriptor)

DECLARE_REFCOUNTED_STRUCT(TBriefJobStatistics)

DECLARE_REFCOUNTED_CLASS(TScheduleAllocationStatistics)

DECLARE_REFCOUNTED_CLASS(TJobExperimentBase)

YT_DEFINE_STRONG_TYPEDEF(TOperationIncarnation, std::string);

class TAutoMergeDirector;
struct TJobNodeDescriptor;

struct IJobSplitter;

struct TJobStatisticsTags;
class TAggregatedJobStatistics;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
