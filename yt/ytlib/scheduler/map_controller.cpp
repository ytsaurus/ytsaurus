#include "stdafx.h"
#include "map_controller.h"
#include "operation_controller.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"

#include <ytlib/ytree/serialize.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TMapController
    : public IOperationController
{
public:
    TMapController(IOperationHost* host, TOperation* operation)
        : Host(host)
        , Operation(operation)
    { }

    virtual void Initialize()
    {
        Spec = New<TMapOperationSpec>();
        Spec->Load(~Operation->GetSpec());

        if (Spec->ShellCommand.empty() && Spec->Files.empty()) {
            ythrow yexception() << "Neither \"shell_command\" nor \"files\" are given";
        }

        if (Spec->In.empty()) {
            // TODO(babenko): is this an error?
            ythrow yexception() << "No input tables are given";
        }
    }

    virtual void Abort()
    {

    }

    virtual void Complete()
    {

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

    virtual void ScheduleJobs(
        TExecNodePtr node,
        std::vector<TJobPtr>* jobsToStart,
        std::vector<TJobPtr>* jobsToAbort)
    {
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

