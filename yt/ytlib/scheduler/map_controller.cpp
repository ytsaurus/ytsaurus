#include "stdafx.h"
#include "map_controller.h"
#include "operation_controller.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"

#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NScheduler {

using namespace NProto;

////////////////////////////////////////////////////////////////////

class TMapController
    : public IOperationController
{
public:
    TMapController(IOperationHost* host, TOperation* operation)
        : Host(host)
        , Operation(operation)
    { }

    virtual TError Initialize()
    {
        Spec = New<TMapOperationSpec>();
        Spec->Load(~Operation->GetSpec());

        if (Spec->In.empty()) {
            // TODO(babenko): is this an error?
            return TError("No input tables are given");
        }

        JobsToStart = 10;

        return TError();
    }

    virtual TAsyncError Prepare()
    {
        return MakeFuture(TError());
    }

    virtual void OnJobRunning(TJobPtr job)
    {

    }

    virtual void OnJobCompleted(TJobPtr job)
    {

    }

    virtual void OnJobFailed(TJobPtr job)
    {

    }

    virtual void OnOperationAborted(TOperationPtr operation)
    {

    }

    int JobsToStart;

    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
        if (JobsToStart == 0)
            return;

        TUserJobSpec userJobSpec;
        userJobSpec.set_shell_comand("cat");

        TJobSpec jobSpec;
        jobSpec.set_type(EJobType::Map);
        *jobSpec.MutableExtension(TUserJobSpec::user_job_spec) = userJobSpec;


        jobsToStart->push_back(Host->CreateJob(Operation, node, jobSpec));
        --JobsToStart;
    }

private:
    IOperationHost* Host;
    TOperation* Operation;

    TMapOperationSpecPtr Spec;
};

TAutoPtr<IOperationController> CreateMapController(
    IOperationHost* host,
    TOperation* operation)
{
    return new TMapController(host, operation);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

