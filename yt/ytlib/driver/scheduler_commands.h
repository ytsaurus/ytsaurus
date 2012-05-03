#pragma once

#include "command.h"

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

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
    : public virtual TUntypedCommandBase
{
protected:
    typedef TSchedulerCommandBase TThis;

    explicit TSchedulerCommandBase(ICommandHost* host);

    void StartOperation(
        TTransactedRequestPtr request,
        NScheduler::EOperationType type,
        const NYTree::TYson& spec);

};

////////////////////////////////////////////////////////////////////////////////

class TMapCommand
    : public TSchedulerCommandBase
    , public TTypedCommandBase<TSchedulerRequest>
{
public:
    explicit TMapCommand(ICommandHost* commandHost);

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TSchedulerRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

class TMergeCommand
    : public TSchedulerCommandBase
    , public TTypedCommandBase<TSchedulerRequest>
{
public:
    explicit TMergeCommand(ICommandHost* commandHost);

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TSchedulerRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

class TSortCommand
    : public TSchedulerCommandBase
    , public TTypedCommandBase<TSchedulerRequest>
{
public:
    explicit TSortCommand(ICommandHost* commandHost);

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TSchedulerRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

class TEraseCommand
    : public TSchedulerCommandBase
    , public TTypedCommandBase<TSchedulerRequest>
{
public:
    explicit TEraseCommand(ICommandHost* commandHost);

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TSchedulerRequestPtr request);
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
    : public TTypedCommandBase<TAbortOperationRequest>
{
public:
    explicit TAbortOperationCommand(ICommandHost* commandHost);

    virtual TCommandDescriptor GetDescriptor();

private:
    virtual void DoExecute(TAbortOperationRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

