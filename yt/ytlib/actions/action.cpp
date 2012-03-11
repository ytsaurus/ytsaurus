#include "stdafx.h"
#include "action.h"
#include "invoker.h"
#include "action_util.h"
#include "cancelable_context.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

IAction::TPtr IAction::Via(IInvoker::TPtr invoker)
{
    YASSERT(invoker);

    return FromMethod(
        &IInvoker::Invoke,
        invoker,
        this);
}

IAction::TPtr IAction::Via(
    IInvoker::TPtr invoker,
    TCancelableContextPtr context)
{
    YASSERT(invoker);
    YASSERT(context);

    auto this_ = MakeStrong(this);
    auto guardedAction = FromFunctor([=] ()
        {
            if (context->IsCanceled())
                return;
            this_->Do();
        });

    return FromFunctor([=] ()
        {
            if (context->IsCanceled())
                return;
            invoker->Invoke(guardedAction);
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
