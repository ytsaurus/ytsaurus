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

////////////////////////////////////////////////////////////////////////////////

class TSchedulerCommandBase
    : public TTypedCommand<TStartOperationRequest>
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

struct TSimpleOperationRequest
    : public TRequest
{
    NScheduler::TOperationId OperationId;

    TSimpleOperationRequest()
    {
        RegisterParameter("operation_id", OperationId);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TAbortOperationCommand
    : public TTypedCommandBase<TSimpleOperationRequest>
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

class TSuspendOperationCommand
    : public TTypedCommandBase<TSimpleOperationRequest>
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

class TResumeOperationCommand
    : public TTypedCommandBase<TSimpleOperationRequest>
{
private:
    virtual void DoExecute();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

