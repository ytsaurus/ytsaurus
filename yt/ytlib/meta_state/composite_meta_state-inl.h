#ifndef COMPOSITE_META_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state.h"
#endif

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TMessage, class TResult>
void TMetaStatePart::RegisterMethod(
    TCallback<TResult(const TMessage&)> handler)
{
    Stroka mutationType = TMessage().GetTypeName();
    auto wrappedHandler = BIND(
        &TMetaStatePart::MethodThunk<TMessage, TResult>,
        Unretained(this),
        MoveRV(handler));
    YCHECK(MetaState->Methods.insert(MakePair(mutationType, wrappedHandler)).second);
}

template <class TMessage, class TResult>
void TMetaStatePart::MethodThunk(
    TCallback<TResult(const TMessage& message)> handler,
    const TMutationContext& context)
{
    TMessage message;
    YCHECK(DeserializeFromProto(&message, context.GetMutationData()));

    handler.Run(message);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
