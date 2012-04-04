#include "stdafx.h"
#include "merge_controller.h"
#include "operation_controller.h"
#include "operation_controller_detail.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

using namespace NYTree;
using namespace NChunkServer;
using namespace NScheduler::NProto;
using namespace NChunkHolder::NProto;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger(OperationsLogger);
static NProfiling::TProfiler Profiler("operations/merge");

////////////////////////////////////////////////////////////////////

class TMergeController
    : public TOperationControllerBase
{
public:
    TMergeController(IOperationHost* host, TOperation* operation)
        : TOperationControllerBase(host, operation)
    { }

    virtual void Initialize()
    {
        TOperationControllerBase::Initialize();

        Spec = New<TMergeOperationSpec>();
        try {
            Spec->Load(~Operation->GetSpec());
        } catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing operation spec\n%s", ex.what());
        }
    }

    virtual void OnJobCompleted(TJobPtr job)
    {
    }

    virtual void OnJobFailed(TJobPtr job)
    {
    }


    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
    }

    virtual i64 GetPendingJobCount()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        return 0;
    }

private:
    TMergeOperationSpecPtr Spec;

    // Custom bits of preparation pipeline.

    virtual std::vector<TYPath> GetInputTablePaths()
    {
        return Spec->In;
    }

    virtual std::vector<TYPath> GetOutputTablePaths()
    {
        std::vector<TYPath> result;
        result.push_back(Spec->Out);
        return result;
    }

    virtual std::vector<TYPath> GetFilePaths()
    {
        return std::vector<TYPath>();
    }

    virtual void DoCompletePreparation()
    {
    }

};

IOperationControllerPtr CreateMergeController(
    IOperationHost* host,
    TOperation* operation)
{
    return New<TMergeController>(host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

