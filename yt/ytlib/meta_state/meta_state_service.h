#pragma once

#include "../rpc/service.h"
#include "../rpc/server.h"

namespace NYT {
namespace NMetaState {

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
    {
        YASSERT(~serviceInvoker != NULL);
    }

    template <
        class TState,
        class TMessage,
        class TResult
    >
    static typename TFuture<TResult>::TPtr CommitChange(
        typename TState::TPtr state,
        const TMessage& message,
        TResult (TState::* changeMethod)(const TMessage&))
    {
        YASSERT(~state != NULL);

        return state->CommitChange(
            message,
            FromMethod(changeMethod, state));
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
        typename TState::TPtr state,
        const TMessage& message,
        TResult (TState::* changeMethod)(const TMessage&),
        void (TThis::* handlerMethod)(TResult result, TContext context))
    {
        YASSERT(~state != NULL);

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
        typename TState::TPtr state,
        const TMessage& message,
        TResult (TState::* changeMethod)(const TMessage&))
    {
        YASSERT(~state != NULL);

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

} // namespace NMetaState
} // namespace NYT
