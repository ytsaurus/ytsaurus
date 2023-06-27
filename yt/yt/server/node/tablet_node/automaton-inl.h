#ifndef AUTOMATON_INL_H_
#error "Direct inclusion of this file is not allowed, include automaton.h"
// For the sake of sane code completion.
#include "automaton.h"
#endif

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void TTabletAutomatonPart::RegisterForwardedMethod(TCallback<void(TRequest*)> callback)
{
    RegisterMethod(BIND(&TTabletAutomatonPart::ForwardedMethodImpl<TRequest>, Unretained(this), callback));
}

template <class TRequest>
void TTabletAutomatonPart::ForwardedMethodImpl(TCallback<void(TRequest*)> callback, TRequest* request)
{
    auto tabletId = FromProto<TTabletId>(request->tablet_id());

    YT_VERIFY(MutationForwarder_);
    try {
        MutationForwarder_->MaybeForwardMutationToSiblingServant(tabletId, *request);
    } catch (const std::exception& ex) {
        const auto* context = NHydra::GetCurrentMutationContext();
        YT_LOG_ALERT(ex, "Failed to forward mutation to sibling servant "
            "(TabletId: %v, Version: %v, Type: %v)",
            tabletId,
            context->GetVersion(),
            context->Request().Type);
    }

    callback(request);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
