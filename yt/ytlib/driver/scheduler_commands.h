#pragma once

#include "command.h"

#include <ytlib/scheduler/map_controller.h>
#include <ytlib/scheduler/merge_controller.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCommandBase
    : public virtual TUntypedCommandBase
{
protected:
    explicit TSchedulerCommandBase(ICommandHost* host);

    void RunOperation(
        TTransactedRequestPtr request,
        NScheduler::EOperationType type,
        const NYTree::TYson& spec);

};

////////////////////////////////////////////////////////////////////////////////

struct TMapRequest
    : public TTransactedRequest
{
    NScheduler::TMapOperationSpecPtr Spec;

    TMapRequest()
    {
        Register("spec", Spec);
    }
};

typedef TIntrusivePtr<TMapRequest> TMapRequestPtr;

class TMapCommand
    : public TSchedulerCommandBase
    , public TTypedCommandBase<TMapRequest>
{
public:
    explicit TMapCommand(ICommandHost* commandHost);

private:
    virtual void DoExecute(TMapRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TMergeRequest
    : public TTransactedRequest
{
    NScheduler::TMergeOperationSpecPtr Spec;

    TMergeRequest()
    {
        Register("spec", Spec);
    }
};

typedef TIntrusivePtr<TMergeRequest> TMergeRequestPtr;

class TMergeCommand
    : public TSchedulerCommandBase
    , public TTypedCommandBase<TMergeRequest>
{
public:
    explicit TMergeCommand(ICommandHost* commandHost);

private:
    virtual void DoExecute(TMergeRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

