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

    explicit TSchedulerCommandBase(const ICommandContextPtr& context);

    void StartOperation(NScheduler::EOperationType type);
};

////////////////////////////////////////////////////////////////////////////////

class TMapCommand
    : public TSchedulerCommandBase
{
public:
    explicit TMapCommand(const ICommandContextPtr& context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TMergeCommand
    : public TSchedulerCommandBase
{
public:
    explicit TMergeCommand(const ICommandContextPtr& context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TSortCommand
    : public TSchedulerCommandBase
{
public:
    explicit TSortCommand(const ICommandContextPtr& context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TEraseCommand
    : public TSchedulerCommandBase
{
public:
    explicit TEraseCommand(const ICommandContextPtr& context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TReduceCommand
    : public TSchedulerCommandBase
{
public:
    explicit TReduceCommand(const ICommandContextPtr& context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceCommand
    : public TSchedulerCommandBase
{
public:
    explicit TMapReduceCommand(const ICommandContextPtr& context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

struct TAbortOperationRequest
    : public TYsonSerializable
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
    explicit TAbortOperationCommand(const ICommandContextPtr& context);

private:
    virtual void DoExecute();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

