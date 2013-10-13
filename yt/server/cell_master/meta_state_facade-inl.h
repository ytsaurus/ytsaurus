#ifndef META_STATE_FACADE_INL_H_
#error "Direct inclusion of this file is not allowed, include meta_state_facade.h"
#endif
#undef META_STATE_FACADE_INL_H_

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

template <class TTarget, class TRequest, class TResponse>
NHydra::TMutationPtr TMetaStateFacade::CreateMutation(
    TTarget* target,
    const TRequest& request,
    TResponse (TTarget::* method)(const TRequest&),
    EAutomatonThreadQueue queue)
{
    return NHydra::CreateMutation<TTarget, TRequest, TResponse>(
        GetManager(),
        GetGuardedInvoker(queue),
        target,
        request,
        method);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
