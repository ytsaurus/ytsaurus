#pragma once

#include "command.h"

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NDriver {

//////////////////////////////////////////////////////////////////////////////

struct TStartOperationRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NYTree::INodePtr Spec;

    TStartOperationRequest()
    {
        RegisterParameter("spec", Spec);
    }
};

typedef TIntrusivePtr<TStartOperationRequest> TSchedulerRequestPtr;

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCommandBase
    : public TTypedCommandBase<TStartOperationRequest>
    , public TTransactionalCommandMixin
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

class TReduceCommand
    : public TSchedulerCommandBase
{
public:
    explicit TReduceCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceCommand
    : public TSchedulerCommandBase
{
public:
    explicit TMapReduceCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortOperationRequest
    : public TRequest
{
    NScheduler::TOperationId OperationId;

    TAbortOperationRequest()
    {
        RegisterParameter("operation_id", OperationId);
    }
};

typedef TIntrusivePtr<TAbortOperationRequest> TAbortOperationRequestPtr;

class TAbortOperationCommand
    : public TTypedCommandBase<TAbortOperationRequest>
{
public:
    explicit TAbortOperationCommand(ICommandContext* context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

