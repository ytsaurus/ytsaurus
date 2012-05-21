#pragma once

#include "command.h"

#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

//struct TSchedulerRequest
//    : public TTransactedRequest
//{
//    NYTree::INodePtr Spec;

//    TSchedulerRequest()
//    {
//        Register("spec", Spec);
//    }
//};

//typedef TIntrusivePtr<TSchedulerRequest> TSchedulerRequestPtr;

//////////////////////////////////////////////////////////////////////////////////

//class TSchedulerCommandBase
//    : public virtual TUntypedCommandBase
//{
//protected:
//    typedef TSchedulerCommandBase TThis;

//    explicit TSchedulerCommandBase(ICommandContext* host);

//    void StartOperation(
//        TTransactedRequestPtr request,
//        NScheduler::EOperationType type,
//        const NYTree::TYson& spec);

//};

//////////////////////////////////////////////////////////////////////////////////

//class TMapCommand
//    : public TSchedulerCommandBase
//    , public TTransactedCommandBase<TSchedulerRequest>
//{
//public:
//    explicit TMapCommand(ICommandContext* commandHost);

//private:
//    virtual void DoExecute();
//};

//////////////////////////////////////////////////////////////////////////////////

//class TMergeCommand
//    : public TSchedulerCommandBase
//    , public TTransactedCommandBase<TSchedulerRequest>
//{
//public:
//    explicit TMergeCommand(ICommandContext* commandHost);

//private:
//    virtual void DoExecute();
//};

//////////////////////////////////////////////////////////////////////////////////

//class TSortCommand
//    : public TSchedulerCommandBase
//    , public TTransactedCommandBase<TSchedulerRequest>
//{
//public:
//    explicit TSortCommand(ICommandContext* commandHost);

//private:
//    virtual void DoExecute();
//};

//////////////////////////////////////////////////////////////////////////////////

//class TEraseCommand
//    : public TSchedulerCommandBase
//    , public TTransactedCommandBase<TSchedulerRequest>
//{
//public:
//    explicit TEraseCommand(ICommandContext* commandHost);

//private:
//    virtual void DoExecute();
//};

//////////////////////////////////////////////////////////////////////////////////

//struct TAbortOperationRequest
//    : public TConfigurable
//{
//    NScheduler::TOperationId OperationId;

//    TAbortOperationRequest()
//    {
//        Register("operation_id", OperationId);
//    }
//};

//typedef TIntrusivePtr<TAbortOperationRequest> TAbortOperationRequestPtr;

//class TAbortOperationCommand
//    : public TTransactedCommandBase<TAbortOperationRequest>
//{
//public:
//    explicit TAbortOperationCommand(ICommandContext* commandHost);

//private:
//    virtual void DoExecute();
//};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

