#pragma once

#include "command.h"

#include <ytlib/scheduler/public.h>

#include <ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NDriver {

//////////////////////////////////////////////////////////////////////////////

struct TDumpInputContextRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NJobTrackerClient::TJobId JobId;
    NYPath::TYPath Path;

    TDumpInputContextRequest()
    {
        RegisterParameter("job_id", JobId);
        RegisterParameter("path", Path);
    }
};

class TDumpInputContextCommand
    : public TTypedCommand<TDumpInputContextRequest>
{
protected:
    typedef TDumpInputContextCommand TThis;

private:
    virtual void DoExecute() override;

};

//////////////////////////////////////////////////////////////////////////////

struct TStraceRequest
    : public TTransactionalRequest
    , public TMutatingRequest
{
    NJobTrackerClient::TJobId JobId;

    TStraceRequest()
    {
        RegisterParameter("job_id", JobId);
    }
};

//////////////////////////////////////////////////////////////////////////////

class TStraceCommand
    : public TTypedCommand<TStraceRequest>
{
protected:
    typedef TStraceCommand TThis;

private:
    virtual void DoExecute() override;

};

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

class TStartOperationCommandBase
    : public TTypedCommand<TStartOperationRequest>
{
protected:
    virtual void DoExecute() override;

    virtual NScheduler::EOperationType GetOperationType() const = 0;

};

class TMapCommand
    : public TStartOperationCommandBase
{
private:
    virtual NScheduler::EOperationType GetOperationType() const override;

};

class TMergeCommand
    : public TStartOperationCommandBase
{
private:
    virtual NScheduler::EOperationType GetOperationType() const override;

};

class TSortCommand
    : public TStartOperationCommandBase
{
private:
    virtual NScheduler::EOperationType GetOperationType() const override;

};

class TEraseCommand
    : public TStartOperationCommandBase
{
private:
    virtual NScheduler::EOperationType GetOperationType() const override;

};

class TReduceCommand
    : public TStartOperationCommandBase
{
private:
    virtual NScheduler::EOperationType GetOperationType() const override;

};

class TMapReduceCommand
    : public TStartOperationCommandBase
{
private:
    virtual NScheduler::EOperationType GetOperationType() const override;

};

class TRemoteCopyCommand
    : public TStartOperationCommandBase
{
private:
    virtual NScheduler::EOperationType GetOperationType() const override;

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

