#pragma once

#include "../rpc/service.h"
#include "../rpc/server.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: move to cpp/inl

class TMetaStateServiceBase
    : public NRpc::TServiceBase
{
protected:
    typedef TIntrusivePtr<TMetaStateServiceBase> TPtr;

    TMetaStateServiceBase(
        IInvoker::TPtr serviceInvoker,
        Stroka serviceName,
        Stroka loggingCategory)
    : NRpc::TServiceBase(
        serviceInvoker,
        serviceName,
        loggingCategory)
    { }

    template <
        class TState,
        class TMessage,
        class TThis,
        class TResult,
        class TContext
    >
    static void CommitChange(
        TThis* this_,
        TContext context,
        TIntrusivePtr<TState> state,
        const TMessage& message,
        TResult (TState::* changeMethod)(const TMessage&),
        void (TThis::* handlerMethod)(TResult result, TContext context))
    {
        TIntrusivePtr<TThis> intrusiveThis(this_);
        NRpc::TServiceContext::TPtr untypedContext(context->GetUntypedContext());
        state
            ->CommitChange(
                message,
                FromMethod(changeMethod, state),
                FromMethod(&TMetaStateServiceBase::OnCommitError, intrusiveThis, untypedContext))
            ->Subscribe(
                FromMethod(handlerMethod, intrusiveThis, context));
    }

    template <
        class TState,
        class TMessage,
        class TThis,
        class TResult,
        class TContext
    >
    static void CommitChange(
        TThis* this_,
        TContext context,
        TIntrusivePtr<TState> state,
        const TMessage& message,
        TResult (TState::* changeMethod)(const TMessage&))
    {
        TIntrusivePtr<TThis> intrusiveThis(this_);
        NRpc::TServiceContext::TPtr untypedContext(context->GetUntypedContext());
        state
            ->CommitChange(
                message,
                FromMethod(changeMethod, state),
                FromMethod(&TMetaStateServiceBase::OnCommitError, intrusiveThis, untypedContext))
            ->Subscribe(
                FromMethod(&TMetaStateServiceBase::OnCommitSuccess<TResult>, intrusiveThis, untypedContext));
    }

private:
    void OnCommitError(NRpc::TServiceContext::TPtr context)
    {
        context->Reply(NRpc::EErrorCode::Unavailable);
    }

    template<class TResult>
    void OnCommitSuccess(TResult, NRpc::TServiceContext::TPtr context)
    {
        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
