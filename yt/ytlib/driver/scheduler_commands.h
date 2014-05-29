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
    typedef TSchedulerCommandBase TThis;

    void StartOperation(NScheduler::EOperationType type);

};

////////////////////////////////////////////////////////////////////////////////

class TMapCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TMergeCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TSortCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TEraseCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TReduceCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TRemoteCopyCommand
    : public TSchedulerCommandBase
{
private:
    virtual void DoExecute() override;

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
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TSuspendOperationCommand
    : public TTypedCommandBase<TSimpleOperationRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

class TResumeOperationCommand
    : public TTypedCommandBase<TSimpleOperationRequest>
{
private:
    virtual void DoExecute() override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

