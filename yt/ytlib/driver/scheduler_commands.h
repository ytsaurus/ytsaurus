#pragma once

#include "command.h"

#include <ytlib/scheduler/map_controller.h>

namespace NYT {
namespace NDriver {

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
    : public TCommandBase<TMapRequest>
{
public:
    TMapCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TMapRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

struct TMergeRequest
    : public TTransactedRequest
{
    TMergeRequest()
    {
        Register("spec", Spec);
    }
};

typedef TIntrusivePtr<TMergeRequest> TMergeRequestPtr;

class TMergeCommand
    : public TCommandBase<TMapRequest>
{
public:
    TMergeCommand(ICommandHost* commandHost)
        : TCommandBase(commandHost)
    { }

private:
    virtual void DoExecute(TMergeRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

