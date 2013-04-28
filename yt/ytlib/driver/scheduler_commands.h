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
    , public TTransactionalCommand
{
protected:
    void StartOperation(NScheduler::EOperationType type);

};

////////////////////////////////////////////////////////////////////////////////

class TMapCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

class TMergeCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

class TSortCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

class TEraseCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

class TReduceCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceCommand
    : public TSchedulerCommandBase
{
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
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

