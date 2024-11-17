#ifndef AUTOMATON_INL_H_
#error "Direct inclusion of this file is not allowed, include automaton.h"
// For the sake of sane code completion.
#include "automaton.h"
#endif

#include "mutation_forwarder.h"

#include <yt/yt/core/misc/codicil.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

template <class TRequest>
void TTabletAutomatonPart::RegisterMethod(
    TCallback<void(TRequest*)> callback,
    const std::vector<TString>& aliases)
{
    NHydra::TCompositeAutomatonPart::RegisterMethod(
        BIND_NO_PROPAGATE(
            &TTabletAutomatonPart::MethodHandlerWithCodicilsImpl<TRequest>,
            Unretained(this),
            callback),
        aliases);
}

template <class TRequest>
void TTabletAutomatonPart::RegisterForwardedMethod(TCallback<void(TRequest*)> callback)
{
    RegisterMethod(
        BIND_NO_PROPAGATE(
            &TTabletAutomatonPart::ForwardedMethodImpl<TRequest>,
            Unretained(this),
            callback));
}

template <class TRequest>
void TTabletAutomatonPart::MethodHandlerWithCodicilsImpl(TCallback<void(TRequest*)> callback, TRequest* request)
{
    TCodicilGuard guard([&] (TCodicilFormatter* formatter) {
        bool firstField = true;
        #define DUMP_GUID_FIELD(snakeCaseName, camelCaseName)                      \
            if constexpr (requires { request->snakeCaseName(); }) {                \
                if (!firstField) {                                                 \
                    formatter->AppendString(", ");                                 \
                }                                                                  \
                firstField = false;                                                \
                formatter->AppendString(#camelCaseName ": ");                      \
                formatter->AppendGuid(FromProto<TGuid>(request->snakeCaseName())); \
            }

        DUMP_GUID_FIELD(tablet_id, TabletId)
        DUMP_GUID_FIELD(table_id, TableId)
        DUMP_GUID_FIELD(transaction_id, TransactionId)
        DUMP_GUID_FIELD(replica_id, ReplicaId)

        #undef DUMP_GUID_FIELD
    });

    callback(request);
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
