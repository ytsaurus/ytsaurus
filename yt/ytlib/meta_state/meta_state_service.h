#pragma once

#include "../actions/action.h"
#include "../rpc/service.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

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

    template <class TContext>
    IParamAction<TVoid>::TPtr CreateSuccessHandler(TIntrusivePtr<TContext> context)
    {
        return FromFunctor([=] (TVoid)
            {
                context->Reply();
            });
    }

    template <class TContext>
    IAction::TPtr CreateErrorHandler(TIntrusivePtr<TContext> context)
    {
        return FromFunctor([=] ()
            {
                context->Reply(NRpc::EErrorCode::Unavailable);
            });
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
