#include <yt/yt/server/controller_agent/private.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

class TOperationControllerBase;

DECLARE_REFCOUNTED_STRUCT(TFinishedJobInfo)
DECLARE_REFCOUNTED_STRUCT(TJobInfo)
DECLARE_REFCOUNTED_CLASS(TJoblet)
DECLARE_REFCOUNTED_STRUCT(TCompletedJob)

DECLARE_REFCOUNTED_CLASS(TTask)

DECLARE_REFCOUNTED_CLASS(TAutoMergeTask)

DECLARE_REFCOUNTED_STRUCT(ITaskHost)

DECLARE_REFCOUNTED_CLASS(TDataBalancer)

DECLARE_REFCOUNTED_STRUCT(TInputTable)
DECLARE_REFCOUNTED_STRUCT(TOutputTable)
DECLARE_REFCOUNTED_STRUCT(TIntermediateTable)

DECLARE_REFCOUNTED_CLASS(TDataFlowGraph)

DECLARE_REFCOUNTED_STRUCT(TBriefJobStatistics)

DECLARE_REFCOUNTED_STRUCT(TScheduleJobStatistics)

class TAutoMergeDirector;
struct TJobNodeDescriptor;

struct IJobSplitter;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

