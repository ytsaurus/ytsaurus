#ifndef COMPOSITE_META_STATE_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state.h"
#endif

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

template <class TMessage, class TResult>
void TMetaStatePart::RegisterMethod(
    TCallback<TResult(const TMessage&)> changeMethod)
{
    YASSERT(!changeMethod.IsNull());

    Stroka changeType = TMessage().GetTypeName();

    auto action = BIND(
        &TMetaStatePart::MethodThunk<TMessage, TResult>,
        Unretained(this),
        MoveRV(changeMethod));
    YCHECK(MetaState->Methods.insert(MakePair(changeType, action)).second == 1);
}

template <class TMessage, class TResult>
void TMetaStatePart::MethodThunk(
    TCallback<TResult(const TMessage&)> changeMethod,
    const TRef& changeData)
{
    YASSERT(!changeMethod.IsNull());

    TMessage message;
    YCHECK(DeserializeFromProto(&message, changeData));

    changeMethod.Run(message);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
