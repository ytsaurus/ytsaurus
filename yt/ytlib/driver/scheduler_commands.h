#pragma once

#include "command.h"

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NDriver {

//////////////////////////////////////////////////////////////////////////////

struct TSchedulerRequest
    : public TTransactedRequest
{
    NYTree::INodePtr Spec;

    TSchedulerRequest()
    {
        Register("spec", Spec);
    }
};

typedef TIntrusivePtr<TSchedulerRequest> TSchedulerRequestPtr;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCommandBase
    : public TTransactedCommandBase<TSchedulerRequest>
{
protected:
    typedef TSchedulerCommandBase TThis;

    explicit TSchedulerCommandBase(ICommandContext* context);

    void StartOperation(NScheduler::EOperationType type);
};

////////////////////////////////////////////////////////////////////////////////

class TMapCommand
    : public TSchedulerCommandBase
{
public:
    explicit TMapCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TMergeCommand
    : public TSchedulerCommandBase
{
public:
    explicit TMergeCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TSortCommand
    : public TSchedulerCommandBase
{
public:
    explicit TSortCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TEraseCommand
    : public TSchedulerCommandBase
{
public:
    explicit TEraseCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortOperationRequest
    : public TConfigurable
{
    NScheduler::TOperationId OperationId;

    TAbortOperationRequest()
    {
        Register("operation_id", OperationId);
    }
};

typedef TIntrusivePtr<TAbortOperationRequest> TAbortOperationRequestPtr;

class TAbortOperationCommand
    : public TTransactedCommandBase<TAbortOperationRequest>
{
public:
    explicit TAbortOperationCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

