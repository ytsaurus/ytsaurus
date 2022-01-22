#pragma once

#include <mapreduce/yt/interface/fwd.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TOperationTracker
{
public:
    TOperationTracker();
    ~TOperationTracker();

    //
    // Add opearation to track.
    void AddOperation(IOperationPtr operation);

    //
    // Wait until all operations are complete.
    //
    // Return vector of all operations.
    //
    // Throw exception if any operation is failed or aborted.
    // Exception is thrown as soon as error operation is detected
    // and don't wait (or abort) other running operations.
    TVector<IOperationPtr> WaitAllCompleted();

    //
    // Wait until all operations are finished.
    //
    // Return vector of all operations.
    //
    // Do not throw exception if any operation is failed or aborted.
    TVector<IOperationPtr> WaitAllCompletedOrError();

    //
    // Wait until any operaiton is complete and return this operation.
    // Throw exception if operation is failed or aborted.
    //
    // Return nullptr if all operations are complete.
    IOperationPtr WaitOneCompleted();

    //
    // Wait until any operaiton is finished successfuly or with error and return this operation.
    // Do not throw exception if operation is failed or aborted.
    //
    // Return nullptr if all operations are complete.
    IOperationPtr WaitOneCompletedOrError();

private:
    class TImpl;
    ::TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
