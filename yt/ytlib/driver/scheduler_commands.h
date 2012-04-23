#pragma once

#include "command.h"

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NDriver {

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

    void WaitForOperation(const NScheduler::TOperationId& operationId);
    void AbortOperation(const NScheduler::TOperationId& operationId);

    void DumpOperationProgress(const NScheduler::TOperationId& operationId);
    void DumpOperationResult(const NScheduler::TOperationId& operationId);

};

////////////////////////////////////////////////////////////////////////////////

struct TMapRequest
    : public TTransactedRequest
{
    NYTree::INodePtr Spec;

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
    NYTree::INodePtr Spec;

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
    : public TSchedulerCommandBase
    , public TTypedCommandBase<TAbortOperationRequest>
{
public:
    explicit TAbortOperationCommand(ICommandHost* commandHost);

private:
    virtual void DoExecute(TAbortOperationRequestPtr request);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

